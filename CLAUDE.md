# Working on neve

Notes for agents and newcomers. Conventions and traps that aren't obvious from
the code, roughly in the order they'll bite you.

## Running the binary

**Use `--stop-time` for bounded runs. Never `timeout`, never `kill -9`.**

```sh
./target/release/neve --network testnet --stop-time 30s --data-dir /tmp/bs
```

`--stop-time` shuts down gracefully: the exit path fsyncs the fjall journal and
checkpoints the blockstore. A hard kill skips both and can leave a torn index,
which at best costs a recovery scan and at worst means rebuilding the store.
Wrapping neve in `timeout` defeats this — `timeout` sends a signal on its own
schedule, so let neve own its shutdown. `Ctrl-C`/`SIGTERM` are also handled
gracefully; `SIGKILL` is the one to avoid.

**neve validates its upstream before it opens any store.** It calls
`eth_chainId` (and the P-chain equivalent) at startup and aborts if that fails —
*before* the RPC server binds. Two consequences: you cannot open a store with no
network, and adding an unreachable chain to `--chains` takes down the chains that
would otherwise have served.

**Be careful with the public endpoints.** Rate limits are **per-IP for the whole
host**, not per chain path — a hard P-chain backfill will throttle a C-chain
instance on the same address. The P-chain endpoint answered a sustained
~14 req/s with HTTP 429 and `Retry-After: 3600`. Point `--p-rpc-url` at your own
node with `--p-request-interval 0` for real fills; keep dev runs short.

## Version control: jj, not git

This is a colocated repo (`.git` and `.jj` both at the root) driven with
[Jujutsu](https://jj-vcs.github.io/). Use `jj` for all VCS operations — raw
`git commit`/`checkout`/`rebase`, and especially `git worktree add`, desync jj's
view of the working copy. Use `jj workspace add` if you need a second tree.

Commits are signed with a hardware key. **Sign as an explicit step before
pushing** (`jj sign -r 'main..@'`) so there's one clear moment to touch the
YubiKey, rather than a surprise prompt blocking mid-push.

Work lands **straight on `main`, without a PR.** GitHub has a ruleset requiring
PRs and locking the branch; pushes report `Bypassed rule violations` and succeed.
That is intentional for now — don't "fix" it by opening PRs, and don't be alarmed
by the warning.

## Before you commit or release

CI runs these; run them locally first. The whole suite is fast (seconds).

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                                    # 145 tests
npx --yes markdownlint-cli2 "docs/*.md" "*.md"
```

CI lints `**/*.md`, which is fine on a fresh checkout but locally also matches
`target/package/**` after a `cargo package`. Scope the glob as above, or ignore
hits under `target/`.

### markdownlint traps

Two rules cause most of the surprise, and both fire on *pre-existing* lines when
you add new ones:

- **MD049 (emphasis style) is `consistent`, inferred per file.** The docs use
  `_underscores_`. Adding a single `*asterisk*` emphasis flips the inferred style
  and makes every pre-existing underscore in that file an error. Match the file.
- **MD060 wants aligned table pipes.** Several docs have wide, hand-padded
  tables. A new row that overruns the existing column widths breaks the whole
  table. Pad the new row to the existing pipe columns rather than reflowing the
  table.

`MD013` (line length) and `MD024` (duplicate headings, for the changelog's
repeated `Added`/`Changed`/`Fixed`) are already disabled in `.markdownlint.json`.

## Release flow

1. Bump `version` in `Cargo.toml`.
2. Add a `CHANGELOG.md` section for it.
3. Commit, sign, push to `main`.
4. Annotated tag: `git tag -a vX.Y.Z <release-commit> -m "neve X.Y.Z — summary"`,
   then push it.
5. `cargo publish`.
6. `gh release create vX.Y.Z --title ... --notes-file ...`.

**Tag the commit you publish from.** `Cargo.toml` has no `include`/`exclude`, so
`cargo package` ships `CHANGELOG.md`, `docs/`, `deploy/`, and `benchmark/` — 58
files. Tagging the version-bump commit and then publishing from a later commit
silently ships a crate that doesn't match its tag.

MSRV lives in `rust-version` and is currently **1.90**, driven by `fjall`, not by
anything neve does. Raising it can wake up MSRV-gated clippy lints (raising it to
1.90 made `collapsible_if` start suggesting let-chains), so re-run clippy after
any MSRV change.

## Storage facts worth knowing

- A stored height is a **JSON array record**, not a bare block: `[block, logs]`
  on the C-chain, `[blockJSON, blockBytesHex, rewards]` on the P-chain. Element 0
  is the block JSON on every chain, which is what lets `oldBlocks`, `/blocks`,
  and the by-hash path stay chain-blind. Stores written before the record format
  (bare block objects) are still readable — the first non-whitespace byte
  disambiguates.
- **Absent is not empty.** `[]` means "ingested, nothing there"; absent means
  "never ingested" and must reach the client as a 421 so it asks a full node.
  Conflating them turns a missing answer into a wrong one. Preserve that
  distinction in any new read path.
- fjall keyspaces are `meta`, `hash_to_height`, `tx_to_block`, all created with
  `KeyspaceCreateOptions::default`. **No KV separation / blob trees**, and every
  key is effectively write-once. Several upstream fjall/lsm-tree bugs turn out
  not to apply for exactly this reason — check before assuming one does.
- Stores are stamped with chain + network identity + record-format version and
  verified on open, so a store can't be opened as the wrong chain or network.
- Ordered-key rule for any *new* index: components you range-scan must be
  **big-endian**. The existing `to_le_bytes()` keys are safe only because every
  current access is a point lookup.

## Docs are the source of truth for plans

`docs/` holds working research documents that lead implementation, and they are
kept honest as things ship:

- `p-chain-indexing-plan.md` — P-chain milestone, what shipped vs. what didn't,
  the public endpoint's rate limits, the core-wallet coverage gaps, and a demo
  cookbook of verified queries.
- `core-wallet-research.md` — the C-chain logs-first activity-feed design.
- `neve-logs-ingestion-plan.md`, `StreamingChangeProofs.md`.

When implementation contradicts a plan, **update the plan and say so** rather
than leaving it stale — several sections are explicitly annotated where building
the thing corrected the research. Mark unverified claims `UNVERIFIED`.
