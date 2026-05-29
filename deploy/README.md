# Deploying neve

Spin up a host that builds and runs `neve` as a systemd service with no manual
steps.

## Files

| File | Role |
| --- | --- |
| `cloud-init.yaml` | EC2 user-data: installs prereqs + the `rkuris` SSH key, clones the repo, runs `bootstrap.sh`. |
| `bootstrap.sh` | Swap, service user, Rust toolchain, `cargo build --release`, installs the unit, `systemctl enable --now`. Idempotent; safe to re-run by hand. |
| `neve.service` | Hardened systemd unit. Runs as the unprivileged `neve` user with state in `/var/lib/neve`. |
| `neve.env` | Operator-editable arguments (`$NEVE_ARGS`). |

## Quick start (AWS)

1. Launch an Ubuntu 22.04/24.04 instance (x86_64 or arm64, e.g. `t4g.small`).
2. Paste `cloud-init.yaml` into **User data**.
3. Size the **root EBS volume** for your retention horizon — the store grows
   monotonically (no pruning): ~0.7 GiB/day without receipts, ~1.4 GiB/day with
   `--receipts`. 100 GiB ≈ 4–5 months without receipts.
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

## Sizing

`neve` is tiny on CPU/RAM (~85 MiB RSS, <1% of a core); **disk is the only real
constraint** and grows without bound. Put `/var/lib/neve` on a volume sized for
your retention, or attach a dedicated data volume mounted there. To rebuild
later, just re-run `sudo bash /opt/neve/deploy/bootstrap.sh` after a
`git -C /opt/neve pull`.
