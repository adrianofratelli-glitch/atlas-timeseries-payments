# Measured numbers

Everything here was measured against the demo Atlas cluster (M20 — 4 GB RAM, 2 vCPU,
MongoDB 9.0), shared with other demonstration databases. Nothing is estimated. Raw output
in `bench-results.json`, `bucket-experiment.json`, `cardinality-experiment.json` and
`../tests/stress-results.json`.

Re-measure rather than copy forward when the cluster, the volume or the traffic shape
changes.

## Live stage capacity — 2026-09-03

`tests/live_ingest_capacity.py` drives the exact API behind the Play button: one mixed
PIX/card/TED `insert_many` per second while `/api/live/overview` aggregates the growing
60-second window. It never clears data; the collection TTL performs retention.

The ramp used 12-second stages and deliberately queried about four times per second.
The 60-second holds used the UI cadence of one query after every one-second wait.

| Workload | Confirmed rate | Batch p95 | Concurrent aggregation p95 | Result |
|---|---:|---:|---:|---|
| stage default, 60 s | **2 281/s** | **820.8 ms** | **768.2 ms** | 140 718 writes, no errors |
| upper hold, 60 s | 3 026/s | 1 000.0 ms | 1 129.6 ms | no errors, but no presentation headroom |
| ramp knee, 12 s | 4 430/s | 1 210.0 ms | 617.2 ms | cycle exceeds the one-second pulse |

The stage default is therefore a **measured presentation operating point**, not the
cluster maximum. `LIVE_TARGET_EPS=1500` is the daily-mean input to the traffic model;
the demo starts at 10:00, where the intraday curve produces approximately 2.3 k/s. The
screen reports the observed confirmed rate, never the nominal input.

## The two decisions that were measured before any code

### Bucket span, and the order events are written in

`queries/bucket_experiment.py` — the same 400 000 events loaded six times.
[`../docs/adr/0001-bucketing.md`](../docs/adr/0001-bucketing.md) has the full table.

| Variant | B/event | Ratio vs plain | Ingest | Health query |
|---|---:|---:|---:|---:|
| plain collection | 52.92 | 1.0× | 28 091/s | 75.2 ms |
| `granularity: "seconds"` | 24.13 | 2.19× | 6 593/s | 32.2 ms |
| `granularity: "minutes"` | 34.16 | 1.55× | 6 211/s | 24.1 ms |
| `bucketMaxSpanSeconds: 3600` | 26.81 | 1.97× | 5 332/s | 32.7 ms |
| `bucketMaxSpanSeconds: 86400` | 33.00 | 1.60× | 6 302/s | **19.2 ms** |
| 86400, written series-contiguous | **10.90** | **4.86×** | **12 308/s** | 20.7 ms |

Write order, isolated on 1 000 000 events over 1 296 routes:

| Writer | B/event | Ingest |
|---|---:|---:|
| generation order, 25 k batches | 33.79 | 8 672/s |
| sorted inside each 25 k batch | 29.88 | 7 905/s |
| sorted globally | **16.84** | **11 733/s** |

**Two numbers to keep honest about.** The plain collection ingests 4.5× faster than the
time series collection — time series trades write throughput for storage and query
shape. And the 2× from global sorting only partly survives in a streaming writer:
buffering 300 events per route on the real load gave **22.86 B/event against 24.26**, six
percent, because a route's bucket is still filled across several flushes. The saving
scales with how much of a bucket the writer delivers per call.

### Where the account goes

`queries/cardinality_experiment.py` — 400 000 events over 2 000 000 accounts, both models
from the identical document list.
[`../docs/adr/0002-cardinalidade.md`](../docs/adr/0002-cardinalidade.md).

| | account as field + index | account inside `meta` |
|---|---:|---:|
| Buckets | **19 127** | 399 924 |
| Events per bucket | 20.9 | **1.0** |
| Index | **3.59 MB** | 98.38 MB |
| Bytes per event | **29.84** | 85.27 |
| Ingestion | **9 143/s** | 1 255/s |
| Account velocity (p50) | **517 ms** | 618 ms |
| p99 per provider (p50) | **427 ms** | 731 ms |

The account in the meta field costs 2.9× the storage, **27× the index** and 7× the write
throughput — and it is slower at the per-account query it exists to accelerate. The
absolute latencies in this table were measured while the cluster was also loading the
main dataset; they are valid as a comparison between the two models, which is what the
ADR needs.

## The base

| | |
|---|---:|
| Events | **44 733 964** |
| Providers | 44 across PIX, card and TED |
| Routes (`meta` combinations) | ~2 900 |
| Accounts | 2 000 000 |
| Period | 7 days at ~75 events/s average |
| Ingestion, 4 route-partitioned writers | 18 861/s (39.5 min) |
| Storage | **905 MB** data + **371 MB** index |
| Bytes per event | **20.23** |
| Buckets | 2 613 915 (**17.1 events per bucket**) |

Bucket occupancy is the number to be honest about: 17 events per bucket against a
theoretical ~1 000. At ~2 900 routes receiving events continuously, buckets close long
before they fill. This is the same effect measured in ADR 0001 and it is why the storage
ratio below is 2.3× rather than the 7× a denser workload would show.

## Storage against a plain collection

| | `payment_events` (time series) | `payment_events_flat` (plain) |
|---|---:|---:|
| Events | 44 733 204 | 6 336 747 |
| Data | 905.0 MB | 290.2 MB |
| Index | 371.1 MB | 384.6 MB |
| Bytes per event (data) | **20.23** | 45.79 |
| Bytes per event (with index) | **28.53** | 106.48 |

**2.26× less storage per event, 3.73× counting the index.**

The index is again where the gap widens: 371 MB covering 44.7 M events against 385 MB
covering 6.3 M. Per event that is 8.3 bytes against 60.7 — an index on a time series
collection indexes buckets, and even at a poor 17 events per bucket that is 7× fewer
entries.

`$collStats` over 2.6 M buckets takes **32 s cold**, which is why the backend warms it
in a thread at startup and caches it for ten minutes. A storage panel that hangs for
half a minute in front of an audience is a defect, not a measurement.

## Query latency

20 runs each, warm, no VPN. **Network floor: p50 17.2 ms** from the presenting host —
subtract it before calling any of these a database number.

| Query | p50 | p95 | above the floor |
|---|---:|---:|---:|
| **Account velocity (1 h / 6 h / 24 h, one pass)** | **28.9 ms** | **35.7 ms** | **11.7 ms** |
| Latency percentiles, channel, 1 h | 302.2 ms | 314.4 ms | 285.0 ms |
| Latency percentiles, provider, 24 h | 904.4 ms | 993.5 ms | 887.2 ms |
| …same query with `$densify`/`$fill` | 944.5 ms | 1 047.2 ms | 927.3 ms |
| Provider health (z-score), 24 h | 1 043.3 ms | 1 376.7 ms | 1 026.1 ms |
| Latency percentiles, channel, 24 h | 6 489.0 ms | 6 954.3 ms | 6 471.8 ms |
| Provider health, 7 d | 7 545.0 ms | 11 061.2 ms | 7 527.8 ms |
| Latency percentiles, provider, 7 d | 8 605.6 ms | 11 073.7 ms | 8 588.4 ms |
| Latency percentiles, **whole channel, 7 d** | — | — | **exceeds 15 s** |

Reading these honestly:

- **The velocity query is the headline.** 28.9 ms p50 over 44.7 M events, 11.7 ms of it
  actual server work. That is what makes the claim "this runs inside the authorisation,
  not on a dashboard" survive contact with a bank's architect. It is a point lookup on
  `{conta_id: 1, ts: 1}` — the model chosen in ADR 0002.
- **Gap reconstruction is nearly free.** `$densify` + `$fill` add 40 ms to a 904 ms
  query.
- **Percentiles cost about 2 s per 250 k events at 24 h.** Measured with and without the
  accumulator: 3 710 ms against 1 625 ms for the same grouping. Beyond 24 h the scan
  dominates.
- **Two queries do not fit the 15 s ceiling**, and both are in the repository on
  purpose. A whole channel over 7 days is 27 M events; the interface never issues it —
  it always scopes by provider — and `latency.serie()` now refuses a channel-wide window
  above 24 h with a `422` and an instruction instead of burning fifteen seconds to
  return a `503`. The provider ranking scans every provider at once: 551 ms at 1 h,
  5.1 s at 6 h, above the ceiling at 24 h, so its window is capped at 6 h with a default
  of 1 h.

### A measurement trap worth knowing

Two of them, both found the hard way:

1. **Benchmarking right after a bulk load measures the load.** A run started immediately
   after the 44.7 M ingest reported a **281 ms network floor** and every query inflated
   to match. Minutes later on a quiet cluster the floor was back to normal.
2. **The network path is part of the number.** An earlier round of these measurements
   ran over a VPN and reported an 8.5 ms floor — *lower* than the 17.2 ms measured
   without it. Every latency here was re-measured on the same path, and the floor is
   printed in the same run for exactly this reason.

## Under mixed load

`tests/stress.py`, three interactive clients (account velocity) for every analytic one
(provider health at 24 h or 7 d), 12 s per level.

| Clients | rps | interactive p50 | interactive p95 | analytic p50 | 429 | 503 |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 63 | 31.9 ms | 57.0 ms | 1 762 ms | 0 | 0 |
| 8 | 117 | 39.0 ms | 64.5 ms | 14 722 ms | 0 | 0 |
| 16 | 124 | 61.7 ms | 117.1 ms | 758 ms | 16 | 2 |
| 32 | 169 | 134.3 ms | 200.7 ms | 758 ms | 78 | 0 |

No 500 and no dead connection at any level. **96 refusals in 7 067 calls (1.4%)** — a
`429` when the analytic queue is full, a `503` when a 7-day query exceeds `maxTimeMS`
under contention. Both are designed behaviour: under saturation the system refuses early
instead of taking the interactive path down with it, and the interactive p95 stays at
200 ms while the analytic path sits at 758 ms.

## Reproducing

```bash
.venv/bin/python queries/bench.py --runs 20                # the latency table
.venv/bin/python queries/bucket_experiment.py              # ADR 0001
.venv/bin/python queries/cardinality_experiment.py         # ADR 0002
.venv/bin/python tests/stress.py --max 32                  # the mixed-load table
.venv/bin/python tests/test_resilience.py                  # 52 hostile cases
```
