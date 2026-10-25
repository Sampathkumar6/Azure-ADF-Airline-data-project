# Design notes

## Integration runtime and linked services
- A **linked service** defines the connection to a data store or compute.
- An **integration runtime (IR)** is the compute that executes the activity. Moving files from a local machine to the cloud needs a **self-hosted IR** installed on that machine, with access granted to the source directories.
- Here the self-hosted IR (`samselfhosted`) serves the `FileServer1` linked service (file system). The other linked services (`AzureSql`, `LS_github`, `LSdatalake`) use the default Azure IR.

## Scaling ingestion without hard-coding
Hard-coding one copy per file does not scale. Two options were considered:
1. Global parameters of object type holding the file mapping.
2. A JSON schema supplied as the value of a `file_name` parameter (preferred for larger file counts).

The on-prem pipeline iterates with `ForEach` over the file list and a parameterized dataset (`DS_onprem_src_param`).

## HTTP ingestion
- Use the **raw** GitHub address, not the HTML page.
- A `Web` activity (method GET) fetches the file; validating it in debug shows an unformatted payload.
- A `Copy` activity follows on success, using an HTTP linked service. **Base URL** is everything up to the domain; **relative URL** is the rest.

## Incremental ingestion
- Watermark state lives in the `monitor` container: an `emptyjson` folder (an empty `{}` file used as a source for the watermark copy) and a `lastload` folder (`lastload.json`).
- `Lookup` activities supply `lastload` (read from `lastload.json`) and `latestload` (`select max(booking_date) from dbo.FactBookings`).
- `Copy_sqldata` selects rows where `booking_date` is greater than `lastload` and at most `latestload`.
- Known limit: `booking_date` has day granularity, so a late row carrying the same date as the watermark would be skipped.
- The `Watermark` copy adds a `lastload` column set to `latestload` and overwrites `lastload.json`.

## Data flows
- Mapping data flows are a low-code layer; Spark runs underneath.
- Silver: source from bronze, then derived columns, casts, casing, selects and filters, ending in an inline **Delta** sink.
- Always place an **alter row** with an upsert condition ahead of the Delta sink.
- Gold: star-schema oriented. Left outer join the fact to each dimension, then aggregate for business views.
- For dynamic column selection, import the projection on the previous step.
- Data flow debug clusters cost money; switch them on only while working.

## Orchestration
`parentPipeline` chains `Onprem_Ingestion`, `API_Ingestion` and `Azuresql_to_datalake` with `Execute Pipeline` activities. A schedule trigger would define when it runs; it is not enabled in this project because incremental runs were triggered manually.
