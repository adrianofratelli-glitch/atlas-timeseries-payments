# Modelling

Why the documents look the way they do. The operational detail — indexes, pipelines,
seeds — is in [`../docs/briefing/02-mongodb.md`](../docs/briefing/02-mongodb.md).

## The split: event, route, opinion

**The event** (`payment_events`, time series) is a fact: one authorised transaction,
written once, never updated. The backend never writes to it at all. Time series
collections restrict updates and deletes precisely because that is the contract.

**The route** (`provedores`) is state. A PSP is onboarded, its SLA changes, it goes
into an incident. Normal collection, unique indexes, upserts.

**The opinion** (`incidents`, `incident_alerts`) is what a human concluded from a
window of events. Separate because it has a different lifecycle and because the change
stream driving the live screen has to watch something that fires once per incident, not
once per transaction.

## The document

```js
{
  ts: ISODate("2026-09-01T14:03:27.412Z"),
  meta: { canal: "pix", provedor: "PSP-014",
          produto: "pix_chave", uf: "SP" },
  valor: 148.90,
  latencia_ms: 96.4,
  aprovado: true,
  erro: null,                  // "AB03" | "05" | ... quando recusado
  conta_id: "C001284471"
}
```

## The cardinality decision

This is the first question any bank asks, so it gets the first answer.

`meta` holds the **route**: channel, provider, product, state. Roughly 2 900
combinations in this dataset. Each distinct meta value is its own bucket series, so the
bucket layer stays small and every observability query — p99 per provider, decline rate
per channel — reads a handful of series.

`conta_id` is a **measurement field** with a secondary index `{conta_id: 1, ts: 1}`.
There are two million accounts here and tens of millions at a real bank. Putting the
account in `meta` turns "a few thousand series" into "one series per account", and the
bucket layer stops being a compression mechanism and becomes a per-account file system.

Both models were built and measured on the same sample —
[`../docs/adr/0002-cardinalidade.md`](../docs/adr/0002-cardinalidade.md) has the table.
The decision is not a preference; it is the difference between the two rows.

## Interval, not counter

The event stores the transaction, not a running total. A pre-aggregated counter answers
only the questions someone anticipated: "declines per provider per minute" is cheap
until somebody asks "declines per provider **per state** for card-not-present between
14:00 and 14:20". Storing the event keeps that question one aggregation away instead of
one sprint away.

The trade-off, stated plainly: raw events cost more storage than counters. That is
exactly what the storage panel measures, and what the bucket format is for.

## `erro` as null, not absent

A declined transaction carries its reason code; an approved one carries `erro: null`.
Explicit null costs a couple of bytes per event and keeps `$group` expressions from
having to distinguish "approved" from "field missing" — a distinction that has produced
wrong decline rates in more than one monitoring system.

## Two constraints that shaped the code

**No user-controlled `_id`, so no upsert.** The generator is idempotent everywhere
except the events, where reloading means `--drop`.

**A time series collection cannot be renamed.** It is a view over `system.buckets.*`
and `renameCollection` fails with `CommandNotSupportedOnView`. Any workflow that stages
data under a temporary name and swaps it at the end has to be written differently here.
