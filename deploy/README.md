# Deploying neve

Spin up a host that builds and runs `neve` as a systemd service with no manual
steps.

## Files

| File              | Role                                                                                                                                          |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `cloud-init.yaml` | EC2 user-data: installs prereqs + the `rkuris` SSH key, clones the repo, runs `bootstrap.sh`.                                                 |
| `bootstrap.sh`    | Swap, service user, Rust toolchain, `cargo build --release`, installs the unit, `systemctl enable --now`. Idempotent; safe to re-run by hand. |
| `neve.service`    | Hardened systemd unit. Runs as the unprivileged `neve` user with state in `/var/lib/neve`.                                                    |
| `neve.env`        | Operator-editable arguments (`$NEVE_ARGS`).                                                                                                   |
| `update.sh`       | Rebuild from `main` and swap the binary in place, archiving the one it replaces.                                                              |
| `rollback.sh`     | Restore an archived binary without rebuilding — and, with no arguments, report which build is actually installed.                             |
| `deploy-lib.sh`   | Shared helpers for those two (locking, archiving, binary identity).                                                                           |

## Quick start (AWS)

1. Launch an Ubuntu LTS instance (x86_64 or arm64, e.g. `t4g.small`; 24.04 and
   26.04 both known good).
2. Paste `cloud-init.yaml` into **User data**.
3. Size the **root EBS volume** for your retention horizon — the store grows
   monotonically (no pruning): ~0.75 GiB/day for the C-chain, so 100 GiB is about
   4½ months of tip growth *plus* whatever history you backfill (see Sizing).
4. Security group: open **22** (SSH). Leave **8545** closed unless you switch
   `neve` to `--rpc-addr 0.0.0.0:8545` (see below).

First boot builds from source (a few minutes on a burstable instance); follow
along with `tail -f /var/log/neve-bootstrap.log`.

## Operating

```sh
systemctl status neve          # is it running?
journalctl -u neve -f          # live logs (summary line, backfill, etc.)
curl -s http://127.0.0.1:8545/health | jq   # block range, disk, memory
```

Change runtime flags by editing `/etc/neve/neve.env` then
`sudo systemctl restart neve`. To serve the JSON-RPC API beyond localhost, set
`--rpc-addr 0.0.0.0:8545` there and open the port.

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

## Sizing

**Measured on a `t4g.small` (2 vCPU, 1.8 GiB) running 0.2.2, C-chain only.** Treat
these as a snapshot, not an invariant — they move with version, workload, and how
many chains are enabled. `curl -s localhost:8545/health | jq .memory` reports your
own instance's figures, which beats trusting this table.

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
  however many blocks you anchor `--backfill-floor` at.
- **Enabling a second chain adds to both.** `--chains c,p` runs a second store and a
  second ingest pipeline in the same process, so expect more RSS and more disk (a
  full P-chain history is ~13 GB; a 1M-height floor is ~0.5 GB). On a 1.8 GiB box
  with the C-chain already near 433 MiB under load, check headroom before turning it
  on rather than after.

To rebuild later, re-run `sudo bash /opt/neve/deploy/bootstrap.sh` after a
`git -C /opt/neve pull` — or use `update.sh`, which does the same thing and keeps a
rollback path.
