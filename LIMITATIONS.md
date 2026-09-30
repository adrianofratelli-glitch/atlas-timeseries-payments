# Limitations

Where the thesis of this repository does not apply. Read before presenting — the
credibility of the conversation depends on raising these before the customer's
architect does.

## The scope decision

This PoV proves the **mechanism**, not production capacity: a native MongoDB time series
collection receives live batches and serves a one-second aggregation while those writes
continue. The collection options, confirmed sample and executed pipeline are visible.

It deliberately does not argue that MongoDB out-ingests a dedicated time series engine
or that the displayed rate represents a bank's peak. Those are sizing and benchmark
questions, and this demo does not answer them.

## What is not demonstrated

**Ingestion at a bank's real peak.** The generator loads in bulk from one host and the
measured ceiling on this cluster is in `queries/benchmarks.md`. There is no Kafka, no
back-pressure story, and no sustained-ingest test at production rates. A bank asking
"can it take our peak PIX second" is asking a question this repository does not answer —
the honest response is a sizing exercise on a dedicated cluster, not this demo.

**Sharding.** The collection is unsharded. Sharding a time series collection is
supported and has real constraints on the shard key, and none of that is exercised here.
Anything said about behaviour beyond a single replica set is opinion.

**A competitive benchmark.** No InfluxDB, no TimescaleDB, no ClickHouse, no Prometheus
ran beside this. The storage comparison is MongoDB against MongoDB — a time series
collection against a plain one, same events — which is a fact about the bucket format,
not a claim about anyone else's product.

**Cardinality at a real bank's account count.** Two million accounts here. A large
retail bank has tens of millions. `docs/adr/0002-cardinalidade.md` measures the *shape*
of the problem — what happens when the account enters the meta field versus staying a
measurement field — at this scale. The direction of the result is structural and does
not reverse with more accounts; the magnitudes would need re-measuring.

**Real traffic.** Fully synthetic data, fictional providers. The volume curves are
shaped to be recognisable to someone from payments — the 4 a.m. trough, the lunch peak,
TED dying at 18:00, lognormal latency with a real tail — but no customer's traffic, no
customer's decline rates and no customer's incident history is behind them.

**Retention at regulatory horizon.** BACEN-scale retention is years. This dataset holds
days. `expireAfterSeconds` and Online Archive are demonstrated as mechanisms; the cost
model at multi-year retention is a sizing conversation, not a measurement here.

**Multi-region and failover.** Single region, single replica set. No read preference
story, no failover timing, no DR claim.

## Constraints of the feature itself

Properties of time series collections, not defects of this project, and each one is a
question a DBA in the room will ask:

- **The bucket parameters are fixed at creation.** `bucketMaxSpanSeconds` and
  `bucketRoundingSeconds` cannot be changed later; getting them wrong means recreating
  the collection and rewriting the data. Measured before any code was written —
  `docs/adr/0001-bucketing.md`.
- **The bucket span is a ceiling, not a promise.** A bucket also closes on a measurement
  count and a size limit, so at high event density per route the span stops being the
  binding constraint.
- **No user-controlled `_id`, therefore no upsert.** Reloading events means dropping and
  rewriting.
- **No unique index.** Deduplication has to happen before the write.
- **Updates and deletes are restricted.** Correcting history is not free, and any design
  that needs to rewrite the past routinely is fighting the storage engine.
- **TTL expires buckets, not documents.** Retention is approximate at the bucket
  boundary.
- **The collection cannot be renamed** — it is a view over `system.buckets.*`.
- **A change stream on the collection fires per event.** Useful for a pipeline, useless
  for driving a screen, which is why the live alert watches `incidents`.
- **`$percentile` with `method: "approximate"`** is a t-digest estimate. It is the mode
  supported over a stream of this size, and the approximation is irrelevant for deciding
  whether a provider degraded — but it is an estimate, and saying otherwise to a risk
  team is how credibility ends.

## Environment

**A shared development cluster.** M20, 4 GB RAM, 2 vCPU, hosting other demonstration
databases at the same time. Every number in `queries/benchmarks.md` was measured under
whatever else was resident. That makes them conservative rather than flattering.

**A network floor of 17.2 ms** from the presenting host to this cluster, measured in the
same run as every latency. Any query reported near that number is measuring the round
trip, not the database. An earlier round of measurements ran over a VPN and reported
8.5 ms — *lower* — which is why the floor is printed alongside the results rather than
assumed.

**The live ingestion is a demonstration device.** A single background thread inside the
API writes one mixed batch of PIX, card and TED events per tick from the same synthetic
model. The stage operating point was measured at 2 281 confirmed events/s for 60 s while
the UI aggregation ran concurrently; this validates the presentation on this cluster,
not production capacity. The observed knee (~4 430/s for 12 s) is explicitly not a sizing
result or a sustained-throughput claim.

**Two queries do not fit the ceiling, by design.** A whole channel over 7 days is 27 M
events and exceeds `maxTimeMS`; `latency.serie()` refuses a channel-wide window above
24 h with a `422` rather than burning fifteen seconds. The provider ranking scans every
provider at once — 551 ms at 1 h, 5.1 s at 6 h, above the ceiling at 24 h — so its window
is capped at 6 h. Both limits are in `queries/benchmarks.md` with the numbers.

**Bucket occupancy is poor and it costs storage.** 17 events per bucket against a
theoretical ~1 000, because ~2 900 routes receive events continuously and buckets close
long before they fill. That is why the storage ratio is 2.3× and not the 7× a denser
workload shows. ADR 0001 measures what changes it and what does not.

**Live detection uses a different reference than the historical view.** The live series
lives for one hour, so learning a baseline from it does not work — measured, a ten-minute
degradation entered the eight-minute baseline window and became its own reference. The
live view compares against the provider's registered `recusa_base`; the historical view
learns the baseline from the data. The interface says which one it used.

**Concurrency is capped, deliberately.** The analytic path has three simultaneous slots
and refuses the excess with a `429` after 750 ms. That is a demo protecting the
interactive path, not a statement about what the cluster could serve with a queue tuned
for throughput.

## What to say when asked

If the question is *raw ingestion at our peak with our cardinality*, the honest answer
is that it is a sizing exercise on a dedicated cluster and that a specialised engine has
a structural advantage at the extreme — co-existence is a legitimate architecture.

If the question is *why do we run five systems to know that an acquirer is degrading and
to score an account's velocity*, this demonstration is the answer to it.
