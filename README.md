# bq-scheduled-query-sequential-backfill

Run BigQuery's scheduled query sequentially from past to current for the creation of snapshot-partitioned master data.

---

## Prepare

```
$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_accout_key.json`
```

## Run

```
$ npm start -- {scheduled_query_name} {start_date} {end_date} {time_zone} {sequential}
```

- `scheduled_query_name`
  - The scheduled query name that you declared.
- `start_date`
  - Inclusive. Format as "yyyy-MM-dd".
- `end_date`
  - Exclusive. Format as "yyyy-MM-dd".
- `time_zone`
  - default: `utc`
  - Format as "utc([+-]\d+)?". For example, input `utc+9` for Asia/Tokyo.
- `sequential`
  - default: `1`
  - `1` makes to run in sequential mode, others for parallel mode.
