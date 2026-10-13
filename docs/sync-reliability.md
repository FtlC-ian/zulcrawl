# Sync reliability

Ideas, not code, adapted from [discrawl's changelog](https://github.com/openclaw/discrawl/blob/main/CHANGELOG.md): 0.13.4 cursor guards and false-complete histories; 0.15.0 freshness/repair; 0.15.1 empty-result explanations; 0.11.4 diagnostics/failure ledger; 0.7.0 JSON control surfaces. No live tail or vector changes.

## Structural constraints

Zulip message IDs are organization-wide, not consecutive per stream. An arithmetic gap between two stream IDs is **not** evidence of missing messages. Deleted/inaccessible messages also cannot be reconstructed from local ranges alone. `sync_state.last_message_id` is a forward checkpoint, not proof that every older message exists. The existing messages primary key, topic uniqueness, sender foreign keys, explicit FTS triggers and per-page message transactions remain authoritative. SQLite has one connection per store and stream workers share that store.

The additive reliability migration transaction creates `sync_health`, `sync_failures` and `(org_id, stream_id, id)` indexing. It seeds legacy successful timestamps with `INSERT OR IGNORE`; unknown attempt/error/repair history stays unknown. Existing checkpoint/event IDs, messages, attachments, embeddings and FTS schema are not replaced. Reopening is idempotent. Checkpoints now advance monotonically, so replay and `--since` cannot move them backwards. `status` and `doctor` acquire the same writer lock as sync before schema initialization; empty-search diagnostics can read the legacy schema without migration.

## Reconciliation policy

Routine sync first audits selected streams (including direct messages) from `oldest`, **at most five API pages of up to 1,001 messages per stream per invocation**, then performs the normal forward sync. Only messages absent from the archive are inserted during this audit, preserving existing snapshots and cached/derived attachment state. This checks actual source history, rather than treating other streams' IDs as holes. Fully scanned histories are audited again after 24 hours. Missing local checkpoint messages bypass that interval. New archives use the existing initial full crawl, which also records completed audit coverage.

The next audit anchor is saved only after page insertion and topic-stat recomputation succeed. Interrupted or failed pages can replay safely; committed earlier pages resume on the next invocation. The audit checkpoint is separate from the forward checkpoint. A partial bounded audit is not advertised as complete: inspect `repair_anchor` and `repair_completed_at`. An audit can recover empty falsely complete archives and internal missing ranges over successive scheduled runs, but cannot promise detection of every hole on its first bounded pass. No source deletions are propagated.

`--full` performs the existing unbounded explicit full crawl; `--since` intentionally disables historical reconciliation for that run to respect the requested cutoff. Config/CLI stream selection is unchanged. The five-page budget bounds history work, not normal bootstrap/incremental work or total network time.

All public and private pages reject empty nonterminal pages, repeated/non-advancing anchors, unordered/invalid IDs and overflow. An inclusive anchor-only **terminal** page remains valid, allowing a no-change incremental sync. Failure never reports success or advances the forward checkpoint.

## Observability

`zulcrawl status [--json]` is local; `zulcrawl doctor [--json]` also verifies API access and database connectivity. JSON schema version 1 contains `streams` and `failures` arrays (including empty arrays). Doctor additionally has `ok` (API/database checks, not a claim of complete history) and `streams_visible`. Each stream exposes counts/bounds, newest message timestamp, forward checkpoint, missing-checkpoint flag, last attempt/success/error, audit progress/completion, exclusion and freshness flags.

Freshness judgment calls for the existing 30-minute scheduler:

- `stale`: never synced, successful sync older than two hours, last sync failed, checkpoint missing, or newest message more than 24 hours behind the last successful sync.
- `quiet`: no message timestamp, or newest message more than 24 hours behind the last successful sync. This is a heuristic warning, **not proof of a gap**; inactive streams may legitimately remain quiet/stale after successful sync.
- `excluded`: config will not capture that stream; historical rows remain searchable. Status covers locally known streams, not undiscovered API streams.

Successful no-change syncs still update freshness and clear last error. Per-stream failures use bounded cancellation-independent cleanup. The global ledger retains the latest 200 failures. Stored errors are safe categories (pagination, cancellation, deadline, other API/storage failure), not untrusted server response text which might contain credentials or private message content. Detailed errors remain in the invoking sync's stderr; generic ledger failures tell operators to retry with progress output. There is no global discovery-error row because a failed stream-list/roster request cannot be attributed to one selected stream.

Empty `search` results keep normal stdout and exit behavior and explain exclusion, absent local data, possibly stale archives, or no FTS match on stderr. Query/filter mismatch is not equated with source absence. Explicitly supplied missing config paths now fail closed instead of silently selecting the default database.

## Verification and test ownership

Run `go build ./... && go test ./... && go vet ./...`. Contracts are owned at the strongest practical boundary:

- Syncer API-to-SQLite tests: public/private repeated, empty and unordered pages stop in at most two requests, return errors, retain the previous forward checkpoint and persist failure/freshness; a seven-message sparse-ID history fills an internal hole across a five-page budget and resumes from the saved anchor; a falsely complete empty archive is recovered even with a recent audit marker. Search assertions prove recovered content is actually indexed. These fakes control source outcomes; they do not prove Zulip server behavior.
- Store migration fixture: an independently constructed legacy checkpoint table survives two migrations, including event IDs and timestamp seeding; a replay cannot lower its forward checkpoint; the failure ledger remains bounded and does not persist injected secret text; successful retry clears errors. Direct fixture SQL is restricted to schema/receipt assertions, not a production test-only seam.
- CLI tests: status/doctor output decodes as JSON with stable keys/arrays, and empty-result stderr differentiates exclusion, stale and absent scopes. These own serialization/diagnostic routing, not sync internals.
- Config loader test: a missing explicit config cannot fall back to the live database.

Existing pagination/FTS/media tests continue to own their distinct contracts. No production seam was added solely for tests: all new store methods have real CLI/sync callers. Regression evidence is the source-to-archive integration outcomes and independently constructed legacy fixture; the old checkpoint-only incremental algorithm cannot retrieve IDs below its checkpoint, has no cursor guard/health ledger, and treats a missing explicit config as defaults. The new assertions fail when those behaviors recur. No duplicate helper-unit cursor tests are needed alongside the integration matrix. Installed `test-value-audit` skill was unavailable to the implementer; this contract/owner/disposition record applies the team protocol's rubric directly and must be checked by the reviewer.

## Deployment after merge

1. Back up the live database with a consistent SQLite backup, and retain the previous binary. Verify the backup before any writable command.
2. Pull merged main in `/Users/debbie/clawd/zulcrawl` and run `go build -o bin/zulcrawl ./cmd/zulcrawl`. Do not deploy a review branch.
3. Run `bin/zulcrawl status --json`, `bin/zulcrawl doctor --json`, then a normal sync when the writer lock is available. First writable initialization applies the additive migration automatically. Inspect audit progress and errors over subsequent runs.
4. The existing `com.zulcrawl.sync` command and 30-minute schedule need **no change**. Routine sync will now audit bounded history automatically.

Rollback can restore the previous binary; the additive tables/index can remain. Do not delete audit state or revert schema while any writer is running. Never use a failed or missing temporary config for fixture checks.
