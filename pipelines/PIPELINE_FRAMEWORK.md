# Parameterized pipeline framework

This is the orchestration core of the platform. One master pipeline ingests every source table without hardcoding a single table name, path, or connection. New sources are onboarded by inserting a row into `etl.ingestion_control`.

## Pipeline inventory

| Pipeline | Role |
|---|---|
| `pl_master_orchestrator` | Entry point. Scheduled daily 02:00 UTC. Calls children in order. |
| `pl_ingest_metadata_driven` | Lookup control table, ForEach over tables, parameterized copy + notebook. |
| `pl_gold_refresh` | Runs gold notebook, warehouse procs, semantic model refresh. |

## 1. Parameters on `pl_master_orchestrator`

| Parameter | Type | Default | Purpose |
|---|---|---|---|
| `LoadDate` | String | `""` | Business date to load. Empty means "yesterday", computed at runtime. Passing a date enables backfills. |
| `Environment` | String | `dev` | Injected per workspace by the variable library. Never hardcode. |
| `IsFullReload` | Bool | `false` | Forces full load on all tables, ignoring watermarks. |
| `TableFilter` | String | `*` | Load one table only (for reruns), e.g. `shipments`. |

First activity is a **Set variable** `v_load_date`:

```
@if(
  equals(pipeline().parameters.LoadDate, ''),
  formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd'),
  pipeline().parameters.LoadDate
)
```

Every downstream activity references `@variables('v_load_date')`, never `utcNow()` directly. This single decision is what makes backfills trivial: rerun with `LoadDate = 2026-03-14` and the whole run replays that day.

## 2. `pl_ingest_metadata_driven` internals

### Lookup: `LKP_GetTables`

Query against the warehouse control table, honoring the filter:

```sql
SELECT table_name, source_folder, source_format, load_type,
       watermark_column, watermark_value, notebook_name
FROM etl.ingestion_control
WHERE is_active = 1
  AND ('@{pipeline().parameters.TableFilter}' = '*'
       OR table_name = '@{pipeline().parameters.TableFilter}')
ORDER BY priority;
```

Uncheck "First row only".

### ForEach: `FE_PerTable`

- Items: `@activity('LKP_GetTables').output.value`
- Sequential: **on** for the trial capacity (batch count 1). On a paid F-SKU, switch to parallel with batch count 4 and let `priority` grouping handle dim-before-fact ordering via two ForEach blocks.

Inside the ForEach, every reference to the current table uses `@item()`:

### If Condition: `IF_LoadType`

```
@and(
  equals(item().load_type, 'incremental'),
  not(pipeline().parameters.IsFullReload)
)
```

**True branch (incremental):** Copy activity `CP_IncrementalToBronze`
- Source path (dynamic content):
```
@concat('Files/landing/', item().source_folder, '/',
        formatDateTime(variables('v_load_date'), 'yyyy/MM/dd'))
```
- Source filter (for formats supporting predicate pushdown, or applied in the notebook):
```
@concat(item().watermark_column, ' > ''', item().watermark_value, '''')
```
- Sink: Lakehouse table `bronze_@{item().table_name}`, table action **Append**.

**False branch (full):** same Copy, path without the date suffix, table action **Overwrite**.

### Notebook activity: `NB_SilverTransform`

- Notebook: `@item().notebook_name` — the notebook itself is selected dynamically from metadata.
- Base parameters:

| Name | Value |
|---|---|
| `p_table_name` | `@item().table_name` |
| `p_load_date` | `@variables('v_load_date')` |
| `p_environment` | `@pipeline().parameters.Environment` |
| `p_is_full` | `@pipeline().parameters.IsFullReload` |

The notebook's first cell is toggled as a **parameters cell**, so these override the defaults (see `notebooks/`).

### Script activity: `SP_UpdateWatermark`

Runs only on notebook success (green dependency). Calls:

```sql
EXEC etl.update_watermark
    @table_name = '@{item().table_name}',
    @new_value  = '@{activity('NB_SilverTransform').output.result.exitValue}';
```

The silver notebook exits with `mssparkutils.notebook.exit(new_watermark)`, so the watermark only advances to the max value **actually loaded**. If the notebook fails, the watermark stays put and the next run picks up where it left off. This is the difference between a demo pipeline and a production one.

### Logging and failure handling

- After each table (success or failure paths both), a Script activity inserts into `etl.pipeline_run_log` using `@pipeline().RunId`, `@utcnow()`, and on the failure path `@activity('NB_SilverTransform').error.message`.
- Failure path ends in an **Office 365 Outlook** activity (or Teams) with a dynamic subject:
```
@concat('[', pipeline().parameters.Environment, '] Ingestion failed: ',
        item().table_name, ' for ', variables('v_load_date'))
```
- The ForEach continues to the next table on failure (dependency from the notification, not a Fail activity), but the master pipeline ends with an If Condition checking the log for failures and raising a **Fail activity** so the run is marked red overall. Partial success should never look green.

## 3. Parent-child invocation

`pl_master_orchestrator` calls children with **Invoke pipeline** activities, passing parameters through:

```
Invoke pl_ingest_metadata_driven
  LoadDate     = @variables('v_load_date')
  Environment  = @pipeline().parameters.Environment
  IsFullReload = @pipeline().parameters.IsFullReload
  TableFilter  = @pipeline().parameters.TableFilter
Wait on completion: true
```

Then `pl_gold_refresh` (gold notebook → warehouse mart procs → semantic model refresh activity), then the data quality notebook, which throws on assertion failure and fails the run before the model refresh if data is bad. Order: ingest → **DQ gate** → gold → refresh. Bad data never reaches the report.

## 4. Environment values via variable library

Create a **Variable library** `vl_freight` in each workspace with:

| Variable | Dev | Test | Prod |
|---|---|---|---|
| `Environment` | dev | test | prod |
| `LakehousePath` | (dev lakehouse id) | (test id) | (prod id) |
| `AlertEmail` | you@bluepeak | you@bluepeak | ops-dl@client |

Pipeline connections and the `Environment` parameter bind to the library, so the identical pipeline definition deploys to all three workspaces and picks up the right values. This is what `fabric-cicd`'s `parameter.yml` handles for item references during deployment; the variable library handles runtime values.

## 5. Demo scenarios to record (Loom)

1. Normal nightly run: show the control table, run with defaults, walk the monitoring view.
2. Backfill: `LoadDate = 2026-03-14`, show the dated landing path resolving in the copy activity input.
3. Targeted rerun: `TableFilter = shipments` after a simulated failure, show the watermark catching up.
4. Onboarding a new source: insert one row into `etl.ingestion_control`, rerun, new table appears in bronze with zero pipeline edits.

Scenario 4 is the money shot. It proves the framework, not just the pipeline.
