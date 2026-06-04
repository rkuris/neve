//! Connection-control primitives for the JSON-RPC server's hand-rolled accept
//! loop (see `rpc::serve`): a read/write idle-timeout stream wrapper.
//!
//! jsonrpsee 0.26 owns its own accept loop and exposes no HTTP/1.1 idle/read
//! timeout (only an HTTP/2 ping keepalive), so an idle inbound keep-alive sits
//! `ESTAB` forever — the fd-leak + slowloris vector documented in memory
//! `neve-fd-leak-rss-findings`. We host jsonrpsee's `TowerService` under our own
//! hyper accept loop (via `to_service_builder`) and wrap each socket in
//! [`IdleTimeout`] to close connections that go quiet.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{Sleep, sleep};

/// Wraps a stream so that a connection with no read **or** write activity for
/// `timeout` is closed. The deadline is checked while a read is pending (the
/// state an idle keep-alive sits in) and reset whenever bytes move in either
/// direction. Resetting on writes too means a healthy server-push WebSocket
/// subscription (which writes but seldom reads) is not falsely reaped — only
/// genuinely silent connections close. A `None` timeout disables the reaping
/// (the stream is passed straight through); see [`IdleTimeout::new`].
///
/// Deliberate behavior for WebSocket (`eth_subscribe`): a subscription is reaped
/// only when BOTH directions are silent for `timeout`. On a live chain each
/// pushed block resets the deadline, so active subscriptions never time out; the
/// one edge case is an upstream feed stall longer than `timeout`, which drops
/// live subscribers (they reconnect). This is intentional — we treat byte-idle
/// uniformly for HTTP and WS rather than adding WS ping/pong liveness.
pub struct IdleTimeout<S> {
    inner: S,
    /// `None` disables reaping (the stream is passed straight through);
    /// otherwise the reset interval and its currently-armed deadline.
    reaper: Option<Reaper>,
}

/// The armed idle deadline plus the interval it resets to on activity.
struct Reaper {
    interval: Duration,
    deadline: Pin<Box<Sleep>>,
}

impl<S> IdleTimeout<S> {
    /// `idle` of `None` disables reaping; `Some(d)` closes the connection after
    /// `d` with no read or write activity.
    pub fn new(inner: S, idle: Option<Duration>) -> Self {
        let reaper = idle.map(|interval| Reaper {
            interval,
            deadline: Box::pin(sleep(interval)),
        });
        Self { inner, reaper }
    }

    /// Push the idle deadline out to `interval` from now. Called on any successful
    /// read or write; a no-op when reaping is disabled.
    fn reset_timeout(&mut self, cx: &mut Context<'_>) {
        if let Some(r) = self.reaper.as_mut() {
            r.deadline.set(sleep(r.interval));
            // Re-register the timer: an un-polled `Sleep` never fires, and after a
            // response write hyper parks on reads without polling us again.
            let _ = r.deadline.as_mut().poll(cx);
        }
    }

    /// Poll the idle deadline, returning `true` if it has expired. Polling also
    /// keeps the timer registered with `cx`. A no-op (`false`) when disabled.
    fn idle_expired(&mut self, cx: &mut Context<'_>) -> bool {
        self.reaper
            .as_mut()
            .is_some_and(|r| r.deadline.as_mut().poll(cx).is_ready())
    }
}

/// The `io::Error` hyper sees when a connection is reaped for being idle; it
/// tears the connection down, freeing the fd.
fn idle_timeout_err() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "connection idle timeout")
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                // Activity — reset the deadline. (A zero-byte read is EOF, which
                // hyper closes on anyway, so resetting is harmless.)
                self.reset_timeout(cx);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                // No bytes yet — fire if we've been idle past the deadline.
                if self.idle_expired(cx) {
                    return Poll::Ready(Err(idle_timeout_err()));
                }
                Poll::Pending
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeout<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        match &r {
            // Activity — reset the deadline.
            Poll::Ready(Ok(_)) => self.reset_timeout(cx),
            // A stalled write (peer not draining) is NOT activity: let the timer
            // run down and reap it. Checked here too because a connection blocked
            // on a write may never be polled for reads, so the read-side check
            // alone would miss it.
            Poll::Pending if self.idle_expired(cx) => {
                return Poll::Ready(Err(idle_timeout_err()));
            }
            _ => {}
        }
        r
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Reproduces the prod stack the isolated tests miss: a keepalive connection
    /// served by hyper-util's `auto::Builder` over an `IdleTimeout` socket. The
    /// client sends one request, reads the response, then goes silent (mimicking a
    /// half-open `ESTAB` leak). The reaper must then close it (client sees EOF); if
    /// it doesn't fire under hyper, the read hangs and the outer timeout trips.
    #[tokio::test]
    async fn hyper_keepalive_idle_connection_is_reaped() {
        use http_body_util::Full;
        use hyper::body::Bytes;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let idle = Duration::from_millis(300);

        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(IdleTimeout::new(sock, Some(idle)));
            let svc = service_fn(|_req| async {
                Ok::<_, std::convert::Infallible>(hyper::Response::new(Full::new(Bytes::from(
                    "ok",
                ))))
            });
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();

        // Drain the response, then go silent and wait for the server to reap us.
        let mut buf = [0u8; 1024];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0, "expected a response");

        // The next read must return EOF once the reaper closes the connection.
        // Allow generous slack over the 300ms idle window.
        let Ok(read) = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf)).await
        else {
            panic!("LEAK REPRODUCED: connection not reaped within 3s of a 300ms idle timeout");
        };
        match read {
            Ok(0) => {}
            Ok(n) => panic!("expected EOF, got {n} more bytes"),
            Err(e) => panic!("expected clean EOF, got error: {e}"),
        }
    }

    /// A stream whose reads are always `Pending` — never data, never EOF — so the
    /// only thing that can complete a read is the idle deadline.
    struct Quiet;
    impl AsyncRead for Quiet {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    /// Yields one byte on the first read, then goes quiet (`Pending`) forever.
    /// Lets us check that the first read resets the deadline.
    struct OnceThenQuiet(bool);
    impl AsyncRead for OnceThenQuiet {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.0 {
                Poll::Pending
            } else {
                this.0 = true;
                buf.put_slice(b"x");
                Poll::Ready(Ok(()))
            }
        }
    }

    #[tokio::test]
    async fn reaper_armed_only_when_enabled() {
        // `None` disables reaping; `Some` arms a deadline.
        assert!(IdleTimeout::new((), None).reaper.is_none());
        assert!(
            IdleTimeout::new((), Some(Duration::from_secs(60)))
                .reaper
                .is_some()
        );
    }

    /// An idle connection is closed with `TimedOut` once the interval elapses.
    /// `start_paused` lets the runtime auto-advance virtual time, so this is
    /// instant and deterministic — no real waiting, no neve, no sockets.
    #[tokio::test(start_paused = true)]
    async fn times_out_after_the_interval_when_idle() {
        let start = tokio::time::Instant::now();
        let mut s = IdleTimeout::new(Quiet, Some(Duration::from_secs(30)));
        let mut buf = [0u8; 8];

        let err = s.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        // It waited the full interval rather than firing early.
        assert!(start.elapsed() >= Duration::from_secs(30));
    }

    /// With reaping disabled (`None`), a quiet connection is never closed: the
    /// read stays pending while a long timer wins the race.
    #[tokio::test(start_paused = true)]
    async fn disabled_reaper_never_times_out() {
        let mut s = IdleTimeout::new(Quiet, None);
        let mut buf = [0u8; 8];

        tokio::select! {
            r = s.read(&mut buf) => panic!("disabled reaper should never resolve, got {r:?}"),
            () = tokio::time::sleep(Duration::from_secs(86_400)) => {}
        }
    }

    /// A read resets the deadline: after data arrives, the timeout is measured
    /// from that point, not from when the connection opened.
    #[tokio::test(start_paused = true)]
    async fn activity_resets_the_deadline() {
        let mut s = IdleTimeout::new(OnceThenQuiet(false), Some(Duration::from_secs(30)));
        let mut buf = [0u8; 8];

        // First read returns immediately (≈ t0) and resets the deadline.
        assert_eq!(s.read(&mut buf).await.unwrap(), 1);
        let after_data = tokio::time::Instant::now();

        // The connection then goes quiet and times out ~30s LATER.
        let err = s.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(after_data.elapsed() >= Duration::from_secs(30));
    }
}
