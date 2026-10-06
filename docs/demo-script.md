# Demo script — 3 minutes

## Before the customer joins

Start the PoV through the portfolio portal and open `http://127.0.0.1:5400`.
The screen must say **Atlas conectado**. Leave the ingestion stopped; Play creates a new
visual session without deleting the events already retained by TTL.

## Opening — 20 seconds

> "I will not show you a dashboard. I will show you MongoDB receiving a time series and
> aggregating it while the data arrives."

Point out that there is one action and one target collection:
`trilho_pagamentos.payment_events_live`.

## Play — 90 seconds

Press **Iniciar ingestão** once.

Read the screen from left to right:

1. The generator creates the synthetic payment events.
2. The number over the rail is the latest `insert_many` batch confirmed by Atlas.
3. The destination is a native time series collection, not a normal collection with a
   timestamp convention.
4. The four numbers come from the running process: events in this execution, observed
   throughput, write confirmation time and aggregation time.

Let the graph grow for at least 20 seconds. Do not click anything else. The fixed
60-second window is deliberate: the audience sees the series being formed instead of a
finished chart stretching to fill its container.

Use the compact strip to connect mechanism to outcome: the live bucket proves how Atlas
stores a route; the strip proves the aggregate effect on the same schema. Its subtitle
says where the numbers come from — **"medido agora neste cluster"** is `$collStats` on
the connected `payment_events` versus `payment_events_flat` (per event, since the flat
sample covers one day), and **"benchmark histórico"** is the versioned full-scale run
(44.7 M measurements → 2.61 M buckets, 2.26× / 3.73×), shown only until the cluster
measurement arrives. Neither is produced by the short live run; say so.

Point to **Documento confirmado**. It changes only after a successful write. The motion
is a representation of confirmed batches; it is not an independent frontend animation
pretending that a write happened.

## Technical proof — 40 seconds

Point to the collection properties read from Atlas: `timeField: ts`, `metaField: meta`
and the one-hour TTL. Open **Ver query / chamada executada** only if the audience asks
how the curve was produced. It contains the pipeline actually run by the API.

## Close — 30 seconds

> "The same MongoDB collection is accepting events and serving a one-second aggregation
> now. This proves the mechanism. Your peak, retention and cardinality determine the
> production sizing; this screen does not pretend to be that benchmark."

Press **Parar ingestão**. The data remains under TTL; stopping a demonstration does not
delete history.

## If something goes wrong

| Symptom | Action |
|---|---|
| API/interface version warning | restart the PoV through the portfolio portal |
| Atlas not connected | stop; verify `/health` before presenting |
| Curve is empty after Play | check the API error and `last_error` in `/api/live/status` |
| Collection says it will be created on Play | press Play once; collection configuration is read back afterwards |
| "Bucket observado" stays on *aguardando* | the server refused both `rawData` and `system.buckets`; the rest of the proof is unaffected — say the physical header is unavailable on this server version |
| `/health` reports 0 events or no flat sample | recreate the data: `ALLOW_DEMO_DB_WRITE=1 .venv/bin/python scripts/reset_demo.py` (full scale, >30 min) |
