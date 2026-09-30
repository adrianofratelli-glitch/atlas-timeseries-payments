# ADR 0001 — Bucket span, and the order events are written in

**Status:** accepted · **Date:** 2026-09-01 · Measured on the demo Atlas cluster
(M20, 4 GB RAM, 2 vCPU, MongoDB 9.0)

## Context

`bucketMaxSpanSeconds` and `bucketRoundingSeconds` are fixed when the collection is
created and cannot be changed afterwards. Getting them wrong means recreating the
collection and rewriting the data.

The workload: a payment rail at ~75 events/s average, `meta` holding the route
(`canal`, `provedor`, `produto`, `uf`) — around 2 900 distinct combinations.

## Measured

`queries/bucket_experiment.py`, the same **400 000 events** loaded six times, 15 runs
per query after warm-up. Raw output in `queries/bucket-experiment.json`.

| Variant | Buckets | ev/bucket | Storage | Index | B/event | Ratio¹ | Ingest | Latency query | Health query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| plain collection | — | — | 21.17 MB | 19.44 MB | 52.92 | 1.0× | 28 091/s | 2 397 ms | 75.2 ms |
| `granularity: "seconds"` | 23 630 | 16.9 | 9.65 MB | 3.76 MB | 24.13 | 2.19× | 6 593/s | 1 680 ms | 32.2 ms |
| `granularity: "minutes"` | 19 447 | 20.6 | 13.66 MB | 3.91 MB | 34.16 | 1.55× | 6 211/s | 1 260 ms | 24.1 ms |
| `bucketMaxSpanSeconds: 3600` | 25 551 | 15.7 | 10.72 MB | 3.65 MB | 26.81 | 1.97× | 5 332/s | 1 376 ms | 32.7 ms |
| `bucketMaxSpanSeconds: 86400` | 19 447 | 20.6 | 13.20 MB | 3.54 MB | 33.00 | 1.60× | 6 302/s | 1 290 ms | 19.2 ms |
| **86400, written series-contiguous** | 19 445 | 20.6 | **4.36 MB** | **1.31 MB** | **10.90** | **4.86×** | **12 308/s** | 1 246 ms | 20.7 ms |

¹ storage per event against the plain collection.

## The finding that mattered more than the span

The last row differs from the one above it **only in the order the events were written**
— same schema, same bucket parameters, the same events, the same bucket count and the
same occupancy. Storage per event drops 3× and ingestion doubles.

That result was surprising enough to be worth isolating, because the first attempt to
apply it did not reproduce. Four variants, 1 000 000 events over 1 296 routes:

| How the writer ordered the events | B/event | Buckets | ev/bucket | Ingest |
|---|---:|---:|---:|---:|
| generation order, 25 k batches | 33.79 | 46 215 | 21.6 | 8 672/s |
| sorted **inside** each 25 k batch | 29.88 | 46 215 | 21.6 | 7 905/s |
| sorted **globally**, 25 k batches | **16.84** | 46 217 | 21.6 | **11 733/s** |

Sorting inside a batch buys 12%. Sorting globally buys **2×** — and the bucket count is
identical in all three, so this is not about how many buckets exist. It is about a
bucket receiving its measurements contiguously, in time order, instead of a few at a
time interleaved with a thousand other series over the bucket's whole lifetime.

The first fix attempt got this wrong in an instructive way: four parallel writers, each
sorting its own batch and partitioned by `(canal, provedor)`, produced **26 B/event** on
the full base — no better than unsorted. Each partition still carried 81 distinct routes
(`produto` × `uf`), so within a partition the series were still interleaved. The unit
that has to be contiguous is the **series**, not the partition.

### How much of that survives in a streaming writer

The 2× above comes from sorting the whole sample before writing — every event of a
series delivered in one contiguous run. A streaming writer cannot do that; it can only
approximate it by buffering per route and flushing when the buffer fills.

Measured on the real load, buffering 300 events per route across four partitioned
writers: **22.86 bytes per event against 24.26** in generation order. Six percent, not
100%.

The gap is the point. With ~860 events per route per day and a 300-event buffer, each
route's bucket is still filled in three separate flushes with a thousand other routes
writing in between. The saving scales with how much of a bucket the writer can deliver
in one call, not with whether the batch happens to be sorted.

The practical form for a customer, then, is specific: **buffer per route long enough to
cover a bucket**, or accept ~24 bytes per event. A Kafka consumer keyed by route with a
generous linger gets most of it; a fan-out consumer, or a per-route buffer far smaller
than the bucket, gets almost none. Saying "just sort your writes" would be a slide that
does not survive contact with the customer's pipeline.

## Decision

```js
db.createCollection("payment_events", {
  timeseries: { timeField: "ts", metaField: "meta",
                bucketMaxSpanSeconds: 86400, bucketRoundingSeconds: 86400 }
})
```

and **the writer buffers per route and flushes a route's events together** —
`generate_events.py` does this by default; `--no-sort` reproduces the generation-order
row.

The one-day span wins the health query (19.2 ms against 24–33 ms) and ties on storage
once sorting is applied. `granularity: "seconds"` looks competitive on raw storage only
because its buckets are so sparse that there is little left to compress badly.

## What the span does **not** buy here

At this event density the span is not the binding constraint: a bucket also closes on a
measurement count and a size limit, which is why a one-day span and a one-minute
granularity produce the same 19 447 buckets. The span matters at low events-per-series;
the write order matters at high fan-out. This workload is the second case.

## Consequences

- The numbers go into `queries/benchmarks.md` and `docs/briefing/02-mongodb.md`. A
  customer asking "why that span" gets this table, not a preference.
- The plain collection ingests **4.5× faster** (28 091/s against 6 302/s unsorted, 2.3×
  against sorted). Time series trades write throughput for storage and query shape, and
  saying otherwise to an architect is how the conversation ends. It is in
  `LIMITATIONS.md`.
- Re-run `queries/bucket_experiment.py` if the route cardinality or the event rate
  changes materially. Both move every row.
