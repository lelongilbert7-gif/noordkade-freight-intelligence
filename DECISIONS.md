# Architecture decision log

Short records of the non-obvious choices. Each answers the question a reviewing engineer would ask.

## ADR-001: Warehouse for finance marts instead of a gold lakehouse view
The carrier scorecard feeds monthly rate negotiations, so finance needs point-in-time snapshots that do not restate when history is reprocessed, and the finance persona works in T-SQL with stored procedures. Snapshot tables in the Warehouse give both. Operational analytics stays on Direct Lake over gold Delta tables where restatement is desirable.

## ADR-002: Metadata-driven ingestion over per-table pipelines
Four sources today, but the framework cost is identical at four or forty. Onboarding a source is one row in `etl.ingestion_control`, which keeps the pipeline definition stable and makes the Git diff for "add a source" trivially reviewable. Trade-off: debugging a ForEach iteration is harder than debugging a dedicated pipeline; mitigated with the run log table and `TableFilter` targeted reruns.

## ADR-003: Watermarks persisted in the Warehouse, advanced by notebook exit value
The watermark only moves to the max timestamp actually written by the silver merge. A failed run leaves it untouched and the next run self-heals. Storing it in the control table (not a pipeline variable) makes state inspectable and manually correctable with one UPDATE.

## ADR-004: Quarantine instead of drop or fail for row-level issues
Row-level problems (negative weights, unknown carriers) go to `silver_quarantine_shipments` with a reason code; the load continues. Dataset-level problems (volume out of band, duplicate keys, FK orphans above threshold) fail the DQ gate and block the gold refresh. Silent drops destroy trust; failing the whole load on 0.4% bad rows destroys availability. Two tiers give both trust and availability.

## ADR-005: Alert extraction via Eventhouse update policy, not a batch job
Reefer excursions are only actionable within minutes, so alert shaping happens at ingest time inside the KQL database. Activator subscribes to the derived `alerts` table rather than raw pings, which keeps rule conditions simple and cheap.

## ADR-006: fabric-cicd for Test/Prod, Git sync for Dev only
Deployment pipelines (the UI feature) cannot run in a PR check, cannot gate on approvals in GitHub, and hide the deployment diff. fabric-cicd from GitHub Actions makes deployment reviewable, repeatable, and auditable. Dev stays Git-synced for fast iteration; Test and Prod are only ever written by the pipeline, never by hand.

## ADR-007: Sequential ForEach on trial capacity
The Fabric trial throttles under concurrent Spark sessions (observed TooManyRequestsForCapacity on prior projects). Batch count 1 trades wall-clock time for reliability. The framework flips to parallel with one setting change on a paid F-SKU; documented rather than assumed.

## ADR-008: Calculation group over duplicated time-intelligence measures
Eight base measures with five time flavors is forty measures to maintain by hand, or one calculation group. The trade-off (format string handling, precedence complexity) is acceptable at this model size and the maintenance saving compounds as measures are added.
## ADR-009: Lowercase-only naming convention for landing folders
OneLake paths are case-sensitive. During the first pipeline run, the copy
activity failed with PathNotFound because the landing folder had been created
as `Carriers` while the control table said `carriers`. Rather than making the
pipeline tolerant of case variations (which hides inconsistency), the
convention is: all landing folder names are lowercase, and the control table
is the single source of truth for spelling. The fix was validated with a
targeted rerun using the TableFilter parameter — one table reloaded without
touching the other three, confirming the framework's surgical-rerun design.
