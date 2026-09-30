# ADR 0002 — Where the account goes: meta field or measurement field

**Status:** accepted · **Date:** 2026-09-01 · Measured on the demo Atlas cluster
(M20, 4 GB RAM, 2 vCPU, MongoDB 9.0)

## Context

This is the first question a bank asks about time series collections: *"we have tens of
millions of accounts — doesn't that explode?"*

Every distinct value of the `metaField` is its own bucket series. The account is the
highest-cardinality dimension in a payment rail and also the one the antifraud velocity
query filters on, so there is a real temptation to put it in `meta`.

Two models of the same data:

- **A** — `meta` holds only the route (`canal`, `provedor`, `produto`, `uf`); `conta_id`
  is a measurement field with a secondary index `{conta_id: 1, ts: 1}`.
- **B** — `conta_id` inside `meta`.

## Measured

`queries/cardinality_experiment.py`, **400 000 events over 2 000 000 accounts**, both
models loaded from the identical document list, 15 runs per query after warm-up. Raw
output in `queries/cardinality-experiment.json`.

| | A · account as field + index | B · account inside `meta` |
|---|---:|---:|
| Buckets | **19 127** | 399 924 |
| Events per bucket | 20.9 | **1.0** |
| Storage | 11.94 MB | 34.11 MB |
| Index | **3.59 MB** | 98.38 MB |
| Bytes per event | **29.84** | 85.27 |
| Ingestion | **9 143/s** | 1 255/s |
| Account velocity query (p50) | **517 ms** | 618 ms |
| p99 per provider (p50) | **427 ms** | 731 ms |

## Decision

**A.** `meta` carries the route; `conta_id` is a measurement field with a secondary
index.

## Why it is not close

- **One bucket per event.** 399 924 buckets for 400 000 events. With the account in
  `meta`, almost every event is the first and only measurement of its own series, and
  the bucket layer stops being a compression mechanism — it becomes per-event overhead.
- **The index is 27× larger** (98.38 MB against 3.59 MB) and larger than the data it
  indexes. An index on a time series collection indexes buckets; when buckets equal
  events, that advantage disappears entirely.
- **Ingestion is 7× slower** (1 255/s against 9 143/s). Opening and closing a bucket per
  event is the dominant cost.
- **And it loses the query it was supposed to win.** Model B exists to make the
  per-account lookup fast, and it is *slower* at it — 618 ms against 517 ms. With one
  measurement per bucket there is no bucket to unpack efficiently; the secondary index
  on the measurement field does the job better.

That last row is the one to keep. The intuition that "filtering on it means it belongs
in the meta field" is wrong in both directions here: it costs 2.9× the storage, 27× the
index and 7× the write throughput, and it does not even pay for itself on the read.

## Consequences

- `conta_id` stays a measurement field, and `schema/indexes.js` creates
  `{conta_id: 1, ts: 1}`. The velocity pipeline in `backend/app/db/velocity.py` matches
  on it directly.
- The rule for `meta` is stated as an invariant in `docs/briefing/01-arquitetura.md`:
  **identity of the route, never the identity of the actor, and never anything mutable.**
- Two million accounts, not tens of millions. The direction of this result is
  structural — it follows from one series per distinct meta value — and does not reverse
  with more accounts; the magnitudes would need re-measuring. That caveat is in
  `LIMITATIONS.md`.
- The absolute latencies above were measured while the cluster was also loading the main
  dataset. They are useful for comparing A against B, which is what this ADR is for, and
  the standalone numbers live in `queries/benchmarks.md`.
