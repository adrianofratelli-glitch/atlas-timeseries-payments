# Changelog

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
