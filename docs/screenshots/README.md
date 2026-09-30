# Screenshots

The screenshots referenced by the public README were captured from a 1600×1000 session
against the real demo cluster after starting the synthetic live feed. Detail images are
cropped from that same interface state.

## Current stage interface

| File | Evidence |
|---|---|
| `08-prova-ao-vivo.png` | complete stage: confirmed ingestion, aggregation, physical bucket and measured bucketization result |
| `09-bucket-fisico.png` | physical bucket header for the latest confirmed document |
| `10-pipeline-executado.png` | complete aggregation pipeline executed by the chart |

The data and providers are fictional. These captures contain no customer identity,
cluster hostname, connection string or secret.

## Engineering archive

Files `01` through `07` document the broader engineering interface used before the stage
was narrowed to one Play and one claim. They remain as development evidence but are not
presented as the current customer-facing UI. The corresponding APIs, benchmarks and ADRs
remain in the repository.
