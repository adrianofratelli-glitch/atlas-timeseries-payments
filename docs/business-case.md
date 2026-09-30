# Business case

How to turn the demonstration into a number, and which numbers this repository refuses
to produce.

## What the demo measures

Three things, all of them on screen and all of them measured rather than asserted:

1. **Storage per event**, against the same events in a plain collection.
2. **Time to detect** a provider degradation — the number of windows between the
   degradation starting and the z-score crossing.
3. **Latency of the velocity feature**, which is the one query that sits inside the
   authorisation path and therefore has a hard budget.

## What it does not measure

**Revenue.** The gap between "a provider degraded" and "the bank lost R$ X" runs through
the bank's own numbers: the volume routed through that provider, the fallback behaviour
when it degrades, the conversion loss on a declined transaction, and how much of that
volume is recovered on retry. None of those live in this repository, and multiplying a
decline rate by an average ticket to produce a headline is doing all four steps
silently.

`CUSTO_MINUTO_INDISPONIVEL` and `TICKET_MEDIO_REFERENCIA` exist in `.env`, default to
zero, and are labelled on screen as customer-informed. If they are zero, the interface
shows no monetary estimate at all. That is deliberate.

## The argument that does not need those numbers

**Systems removed.** The reference architecture for this workload is a time series
database for the metrics, a relational database for the provider registry, a cache for
the dashboard, a search engine for incident history, and a feature store for the
antifraud velocity. This runs on one cluster. Five sets of credentials, backups,
upgrades, on-call rotations and integration tests become one.

**Questions that stop being projects.** "Decline rate for this acquirer, for
card-not-present, in São Paulo, between 14:00 and 14:20" is one aggregation here. In a
pre-aggregated metrics stack it is a new counter, a deploy, and a wait until tomorrow's
data. The value is not the query being fast; it is the question being answerable at all.

**The feature store that is not a second system.** Account velocity is computed from the
same events, in the same cluster, with a measured latency in
`queries/benchmarks.md`. Every feature store is a copy of data that already exists,
plus a synchronisation job, plus the day the copy is wrong.

**Storage, measured.** `queries/benchmarks.md` has bytes per event and the ratio
against a plain collection, measured on this cluster. That translates directly into a
disk line on an invoice and requires nobody to believe an estimate.

## What to ask the customer for

If they want a business case with their own numbers, these are the inputs, and none of
them can be invented on our side:

1. Events per second on the rail, at peak and average, per channel.
2. Retention required by regulation and by the fraud team, separately — they differ.
3. The list of systems in the current observability and feature stack, with licence and
   operating costs.
4. Current time to detect a provider degradation, and how it is detected today.
5. Cardinality: how many accounts, how many providers, how many distinct routes.

With those, the model is arithmetic. Without them, the honest deliverable is the
demonstration plus the measured storage and latency — already more than most proposals
put on the table.
