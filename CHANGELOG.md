# Changelog

## 1.1.1 (2026-10-09)

- Live clear no longer races the writer: start/stop/clear share a lifecycle lock, a write gate with an epoch makes clear wait for the in-flight batch and discard any later batch of the old session, the writer thread is joined, and `payment_events_live` is recreated as a time series with TTL and indexes before writes resume. A late insert can no longer recreate it as a plain collection. A concurrent start + clear no longer answers HTTP 500 (`cannot join thread before it is started`). Clear answers `409` instead of a false "cleared" when an in-flight write does not finish in time.
- The insert path checks, under the gate, that the live collection is a time series; `ensure_collection` replaces a plain collection left behind by the old race.
- `LIMITATIONS.md` rewritten from measurement: `queries/feature_probes.py` executes every quoted constraint on the connected server (9.0.4): explicit `_id` accepted but not unique, bucket span can grow but not shrink, change streams unsupported (code 115), rename accepted. ADR 0001, schema notes and briefing aligned.
- Vite binds to `127.0.0.1`, the address `start.sh` probes; with `localhost` resolving to `::1` the launcher reported the frontend as not ready.
- Regression suite `tests/test_live_lifecycle.py` (real cluster, `*_test`).

## 1.1.0 (2026-10-06)

- UI: MongoDB 2026 "Dark Stage v4" layout (local Special Gothic / Source Code Pro fonts, staircase and grid motifs, staggered motion).
- Physical bucket panel works on MongoDB 8.2+/9.0: the header is read with `find(..., rawData: true)` instead of the now-blocked `system.buckets.*`.
- The bucketization strip shows `$collStats` measured on the connected database; the historical benchmark is only a labelled fallback.
- Live ingestion: retrying a batch after a lost acknowledgement no longer duplicates events (time series has no unique `_id`), throughput is computed from the wall-clock tick so it does not overstate the rate under backpressure, and the per-second curve no longer saw-tooths.
- `scripts/reset_demo.py`: one idempotent reset for every collection and index, refusing a non-`_test` database without `ALLOW_DEMO_DB_WRITE=1`, with `--resume` for an interrupted event load. `schema/indexes.js` (mongosh) replaced by `common.INDEXES`.
- API rejects `inf`/`nan` and unknown body fields, and the ranking reports when its window was clamped.
- Adversarial suites in `tests/adversarial/` (API and direct ingestion against the cluster).
- Reduced-scale re-measurement recorded in `queries/benchmarks.md` and ADR 0001.

## 1.0.0 (2026-09-30)

First public release.

- Repository rebuilt with a clean, single-commit history.
- English README and repository description, with screenshots captured against a real Atlas cluster.
- MIT license.
- Internal notes, presentation decks, test-output snapshots, and tooling configuration removed from the repository.
