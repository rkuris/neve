# Deploying neve

Spin up a host that builds and runs `neve` as a systemd service with no manual
steps.

## Files

| File                  | Role                                                                                                                                          |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `cloud-init.yaml`     | EC2 user-data: installs prereqs + the `rkuris` SSH key, writes a minimal `/etc/neve/config.toml`, clones the repo, runs `bootstrap.sh`.       |
| `bootstrap.sh`        | Swap, service user, Rust toolchain, `cargo build --release`, installs the unit + config, `systemctl enable --now`. Idempotent; re-run safely. |
| `neve.service`        | Hardened systemd unit. Runs as the unprivileged `neve` user with state in `/var/lib/neve`.                                                    |
| `config.toml.example` | The annotated reference config, installed to `/etc/neve/config.toml.example`. Same text as `neve --print-config-example`.                     |
| `neve.env`            | `NEVE_UPSTREAM_TOKEN`, plus `$NEVE_ARGS` as an emergency override channel. Not where tuning lives.                                            |
| `update.sh`           | Rebuild from `main` and swap the binary in place, archiving the one it replaces.                                                              |
| `rollback.sh`         | Restore an archived binary without rebuilding — and, with no arguments, report which build is actually installed.                             |
| `deploy-lib.sh`       | Shared helpers for those two (locking, archiving, binary identity).                                                                           |

## Quick start (AWS)

1. Launch an Ubuntu LTS instance (x86_64 or arm64, e.g. `t4g.small`; 24.04 and
   26.04 both known good).
2. Paste `cloud-init.yaml` into **User data**.
3. Size the **root EBS volume** for your retention horizon — the store grows
   monotonically (no pruning): ~0.75 GiB/day for the C-chain, so 100 GiB is about
   4½ months of tip growth *plus* whatever history you backfill (see Sizing).
4. Security group: open **22** (SSH). `cloud-init.yaml` binds `0.0.0.0:8545`, so
   open **8545** only if you want that reachable; otherwise set
   `addr = "127.0.0.1:8545"` under `[server]` in `/etc/neve/config.toml`.

First boot builds from source (a few minutes on a burstable instance); follow
along with `tail -f /var/log/neve-bootstrap.log`.

**A fresh box runs both chains.** The config `cloud-init.yaml` writes has a
`[chains.c]` and a `[chains.p]` table, and the presence of a table is what
enables that chain. The C-chain anchors at the first block it sees; the P-chain
fills from genesis (`chains.p.backfill_floor` defaults to `0`), which is ~25.3M
mainnet heights and the reason the disk figures below are not the whole story —
see [Sizing](#sizing). For a C-chain-only box, delete the `[chains.p]` table.

## Operating

```sh
systemctl status neve          # is it running?
journalctl -u neve -f          # live logs (summary line, backfill, etc.)
curl -s http://127.0.0.1:8545/health | jq   # block range, disk, memory
```

Change settings by editing `/etc/neve/config.toml` then
`sudo systemctl restart neve` — the unit runs
`neve --config /etc/neve/config.toml $NEVE_ARGS`. To serve the JSON-RPC API
beyond localhost, set `addr = "0.0.0.0:8545"` under `[server]` and open the
port. Two commands answer "what is this box actually configured to do":

```sh
neve --config /etc/neve/config.toml --print-config   # resolved, secrets redacted
cat /etc/neve/config.toml.example                    # every key, annotated
```

`--print-config` is the one to reach for, because it resolves the whole
precedence chain — built-in defaults, `[defaults]`, `[chains.<x>]`, environment,
then command line — and a box that still has flags in `$NEVE_ARGS` is exactly
the case where the file alone misleads you.

### The upstream token

`/etc/neve/config.toml` carries no secret. The rate-limit bypass token goes in a
root-owned file the service user can read, referenced from the config:

```sh
sudo install -m 0640 -o root -g neve /dev/null /etc/neve/token
printf '%s' '<token>' | sudo tee /etc/neve/token >/dev/null
```

```toml
[upstream]
token_file = "/etc/neve/token"
```

neve appends it to every upstream URL it builds. `NEVE_UPSTREAM_TOKEN` in
`/etc/neve/neve.env` does the same job and `token_file` wins if both are set,
but the file is preferred in production: an environment variable is inherited by
every child process and readable through `/proc/<pid>/environ`, while the file is
read once at startup. A flag would be worse still — `/proc/<pid>/cmdline` is
world-readable — which is why there is no token flag. The token is held in a type
that renders `<redacted>` in both `Debug` and `Display`, and `--print-config`
prints the path rather than the value.

Configuring a token also turns **off** the default host-wide rate cap
(`upstream.max_rps`, 25 req/s otherwise): a bypass token is the reason to have
one. The effective cap is logged once at startup, along with which of those cases
applied.

### Which build is actually running?

`rollback.sh` with no arguments answers this, and it is the quickest way to find
out — listing is its default precisely because you usually want to know what is
installed before changing it:

```console
$ sudo bash /opt/neve/deploy/rollback.sh
installed binary in /usr/local/bin:
     neve-20260812T033359Z-f6b77f6    neve 0.2.2 (f6b77f6)        17M

archived binaries in /var/backups/neve (newest first):
  1) neve-20260812T033359Z-06c4d70    neve 0.2.2 (06c4d70)        17M
  2) neve-20260811T182923Z-a1787b8    neve 0.2.2 (a1787b8)        17M
  3) neve-20260811T180648Z-e2a3dfb    neve 0.2.2 (e2a3dfb)        17M
  4) neve-20260811T174342Z-724acbd    neve 0.2.1                  17M

to roll back:  sudo bash /opt/neve/deploy/rollback.sh <number|sha>
```

Two things make that identity trustworthy rather than merely plausible:

- **It asks each binary what it is** — `neve --version` reports the crate version
  *and* the commit it was built from, so the listing never has to trust a filename.
  A binary installed by hand still identifies itself correctly; an older one that
  predates the embedded commit shows just its version, as `neve 0.2.1` does above.
- **The `<- currently installed` marker compares bytes**, not labels, so a binary
  replaced out-of-band cannot be mislabelled as something it is not.

`systemctl status` confirms the *unit* is up but says nothing about which build;
`/health` and `neve_build_info` report the running one but need a live instance.
`rollback.sh` works on binaries at rest, which is what you want when deciding
whether a deploy actually landed.

### Updating and rolling back

```sh
sudo bash /opt/neve/deploy/update.sh    # rebuild from main, swap, archive the old binary
sudo bash /opt/neve/deploy/rollback.sh  # list; then re-run with <number|sha> to revert
```

Rolling back is a file copy rather than a compile, which matters on a small
instance: rebuilding means minutes of downtime you are trying to *end*, not
start. The two scripts serialize against each other, and the swap is atomic.
Archives live in `/var/backups/neve` — off `PATH`, outside the repo checkout —
with the newest 5 retained (`ARCHIVE_DIR`, `ARCHIVE_KEEP` to override).

Updating a box provisioned before the config file existed writes a minimal
`/etc/neve/config.toml` for it, because the refreshed unit passes `--config`, and
refreshes `/etc/neve/config.toml.example`. It deliberately does **not** translate
`$NEVE_ARGS`: those flags are deprecated but still honored and still outrank the
file, so writing them into it as well would create two sources that can disagree.
The box keeps behaving as it did; move the settings across at your leisure and
empty `NEVE_ARGS` when you do. The one change to expect is chain selection — a
box whose `$NEVE_ARGS` never said `--chains` now runs the P-chain as well, since
the written config enables both. Delete the `[chains.p]` table to opt out.

## Sizing

**Measured on a `t4g.small` (2 vCPU, 1.8 GiB) running 0.2.2, C-chain only, with the
system allocator.** Treat these as a snapshot, not an invariant — they move with
version, workload, and how many chains are enabled, and the RSS figure in particular
is expected to fall now that jemalloc is the default allocator (it was the evidence
for that change). `curl -s localhost:8545/health | jq .memory` reports your own
instance's figures, which beats trusting this table.

| | Backfilling (~48 blocks/s) | Notes |
| --- | --- | --- |
| RSS | **~433 MiB** | Dominated by connection buffers and allocator fragmentation, so it tracks request concurrency more than store size |
| CPU | **~14% of one core** | Backfill is a single serial fetch loop; it does not scale across cores |
| Disk | **~9.8 KiB/block** | 37.3 GiB for 4.09M blocks |
| Disk growth at tip | **~0.75 GiB/day** | ~0.95 blocks/s × 9.8 KiB |

Earlier revisions of this file claimed ~85 MiB RSS and <1% of a core. Those were
measured on an idle, caught-up mirror; under an active backfill the real numbers are
several times higher, and the idle figures have not been re-verified for 0.2.2.
**Size for the backfilling case** — that is when the host is under load and when a
too-small instance actually fails.

`bootstrap.sh` also creates a 2 GiB swapfile, which is part of why a 1.8 GiB
instance copes at all — it was lightly used (~77 MiB) at the time of measurement.
Swap covers a burst; it does not substitute for headroom.

Two things to plan for beyond the table:

- **Disk still grows without bound** (no pruning). Put `/var/lib/neve` on a volume
  sized for your retention, or attach a dedicated data volume mounted there.
  Backfilled history is on top of tip growth: a full C-chain fill is ~9.8 KiB ×
  however many blocks you anchor `chains.c.backfill_floor` at. It defaults to
  `"tip"` — anchor at the first live block and fill forward only — so a C-chain
  store only grows into history if you ask it to.
- **The second chain is on by default now, and adds to both — but not
  proportionally.** A `[chains.p]` table (which is what `cloud-init.yaml` writes)
  runs a second store and a second ingest pipeline in the same process, and
  `chains.p.backfill_floor` defaults to `0`, so it fills from genesis. Disk is
  straightforward: a full P-chain history is ~13 GB, a 1M-height floor ~0.5 GB. RSS
  should rise by tens of MiB rather than anything like doubling — the C-chain's
  433 MiB is mostly connection buffers and in-flight fetch state, and a P-chain
  height is ~3.7 KiB on the wire and ~520 B on disk against the C-chain's ~25 KiB
  and ~9.8 KiB. For reference, the same host measured 972 MiB `MemAvailable` and
  929 MiB of (reclaimable) page cache while the C-chain backfilled, with 76 MiB of
  2 GiB swap in use. Watch `/health`'s `memory.physical_human` after enabling rather
  than provisioning for a doubling that is unlikely to arrive.

  Where the P-chain history comes *from* decides whether that fill takes minutes
  or months: against the public endpoint it is measured in months, so fill from
  your own node or from another neve and then follow the tip. The recipes are in
  the top-level README under
  [Bootstrapping the P-chain from genesis](../README.md#bootstrapping-the-p-chain-from-genesis).

To rebuild later, re-run `sudo bash /opt/neve/deploy/bootstrap.sh` after a
`git -C /opt/neve pull` — or use `update.sh`, which does the same thing and keeps a
rollback path.
