# ADR 0003 — Keep the chart pipeline on block processing, and show the plan

**Status:** accepted · **Date:** 2026-11-12 · Measured on the demo Atlas cluster
(shared M20, MongoDB 9.0.7) against the reduced `trilho_pagamentos_test` dataset
(2.53 M events over 3 days)

## Context

MongoDB 8.0 added block processing for time series: eligible stages run in the
slot-based engine (SBE) over columnar blocks unpacked from the buckets, instead of one
document at a time. It is automatic and has no flag — which also means a pipeline can
lose it without anyone noticing. The only evidence is the plan.

The live chart pipeline was:

```text
$match ts → $group by $dateTrunc(second) → $set {ts: "$_id"} → $sort {ts: 1} → $project → $limit 60
```

`explain` showed it running entirely in the classic engine (`$cursor` + unpack + `$group`
as separate classic stages). Removing stages one at a time isolated the cause: the
`$sort` on `ts`, a field created by `$set` after the group, cannot be pushed into SBE, and
the planner then keeps the whole pipeline classic. Sorting on the group key keeps SBE.

## Decision

1. The pipeline sorts on `_id` (the group key), applies `$limit`, and renames `_id` to
   `ts` in the final `$project`. Same documents, same order, same 60-point cap.
2. The query drawer shows the plan the server chose: `GET /api/live/plan` runs `explain`
   on the same pipeline at `queryPlanner` verbosity (planned, not executed), cached for
   60 s, and the screen prints the engine and the block stages. On demand rather than per
   poll, because the plan does not change every second and an `explain` per poll would
   double the round trips of the interactive path.

## Measured

`queries/block_processing.py --runs 15`, both shapes over the same window, outputs
compared for equality, latency end to end from the presenting host. The network floor was
measured in the same run; it was unusually high that day (see `LIMITATIONS.md`), so the
small window is dominated by the round trip.

Network floor (`ping`, same run): p50 310 ms. Raw output: `queries/block-processing.json`.

| Window (events) | Previous shape: engine · p50 / p95 | Current shape: engine · p50 / p95 | p50 ratio |
|---|---|---|---:|
| 1 h (~36 k) | classic · 1,072 / 1,258 ms | SBE + block · 655 / 823 ms | 1.64× |
| 24 h (~860 k) | classic · 7,175 / 12,561 ms | SBE + block · 4,153 / 4,605 ms | 1.73× |
| 72 h (2.53 M) | classic · 28,461 / 30,496 ms | SBE + block · 23,425 / 29,242 ms | 1.21× |

The 72-hour row is honest noise as much as signal: it groups 259,200 one-second bins,
the fastest single classic run (16.6 s) beat the fastest SBE run (20.6 s), and the shared
cluster was busy with other work. The chart only ever asks for 60 seconds; the 1 h and
24 h rows are the relevant ones and both are well above the noise.

Both shapes returned identical output in every window. The current shape plans
`ts_bucket_to_cellblock → block_group → block_to_row`; the previous one plans no block
stage at all.

## Consequences

- A future edit to the chart pipeline that reintroduces a sort on a derived field is
  caught offline (`tests/test_offline_core.py`) and visible on screen (the drawer would
  say "motor clássico").
- The gain is workload- and version-dependent. The numbers above are this cluster on this
  day, not a promise; re-run the script before quoting a ratio. Other pipelines in
  `app/db/` (latency percentiles, `$setWindowFields`, `$densify`/`$fill`) were not
  re-shaped here.
