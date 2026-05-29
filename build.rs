//! Capture the git commit at compile time so the binary can expose it via the
//! `neve_build_info` Prometheus series. Emits `NEVE_GIT_COMMIT` for `env!` to
//! pick up; falls back to "unknown" outside a git checkout (e.g. a crates.io
//! build) so the `env!` never fails to resolve.

use std::process::Command;

fn main() {
    // Re-run when HEAD moves (new commit / checkout) so the embedded SHA stays
    // current; harmless if .git/HEAD is absent.
    println!("cargo:rerun-if-changed=.git/HEAD");

    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=NEVE_GIT_COMMIT={commit}");
}
