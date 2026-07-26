# Noordkade Freight: Operations Intelligence Platform

End-to-end Microsoft Fabric analytics platform for Noordkade Freight, a fictional Rotterdam-based 3PL. All data is synthetically generated; no real company data is used.

The platform covers batch shipment analytics through a medallion lakehouse and T-SQL warehouse, live fleet telematics through Eventstream and a KQL Eventhouse, a Direct Lake semantic model with calculation groups and RLS, and automated CI/CD across three isolated workspaces (Dev, Test, Prod) using fabric-cicd.

Built by Gilbert Kiptoo (BluePeak Analytics). Certifications: DP-600, PL-300.

## Status

The batch platform and its three-environment deployment pipeline are complete and running. A single repository, authored in NKF-Dev, deploys through fabric-cicd to NKF-Test and NKF-Prod, with every notebook, pipeline, semantic model, and report rebound to its target environment automatically. Bronze, Silver, and Gold layers build from deployed artifacts in each environment.

The streaming components (Eventstream, Eventhouse, Activator) are on the roadmap below.

## Architecture

**Batch.** CSV landing (date-partitioned) → metadata-driven Data Pipeline → Bronze → Silver (PySpark merge, quarantine) → data-quality gate → Gold star schema → Direct Lake semantic model → executive report.

**Streaming (planned).** Telematics simulator → Eventstream custom endpoint → Eventhouse (update policy shapes alerts at ingest) → Real-Time Dashboard + Activator rules (reefer temperature excursions, harsh braking, escalation on alert bursts).

**Governance.** Three workspaces (Dev/Test/Prod), feature-branch Git workflow, and environment-parameterized deployment through fabric-cicd. Deployments are scoped: `core` deploys notebooks and pipelines, `full` adds the semantic model and report. Lakehouses and warehouses are provisioned per environment and deliberately kept out of the deploy scope, since they hold data rather than logic.

## How environment rebinding works

This is the heart of the CI/CD design and the part that took the most engineering to get right.

Every Fabric item authored in Dev carries hardcoded references to Dev's own lakehouse and warehouse — OneLake paths in the semantic model, default-lakehouse GUIDs in notebook metadata, and warehouse connection details in the pipeline. Deploying those definitions unchanged would leave Test and Prod reading from Dev.

`cicd/parameter.yml` handles this with a set of `find_replace` rules, one per shape of reference:

- The semantic model's Direct Lake source appears as a single `workspace/lakehouse` OneLake path.
- Notebook and pipeline metadata carry the lakehouse and workspace GUIDs as separate fields.
- The pipeline's warehouse connection carries both a SQL endpoint hostname and an artifact ID.

Each rule maps the Dev value to a Test and a Prod value, scoped to the item types where it appears. fabric-cicd applies the substitution at deploy time, so the same committed definition lands correctly in every environment.

## What this demonstrates

| Capability | Where |
|---|---|
| Metadata-driven parameterized pipelines | `pipelines/PIPELINE_FRAMEWORK.md`, `pipelines/00_control_table.sql` |
| Incremental loads with self-healing watermarks | `notebooks/nb_silver_shipments.py` |
| Data quality gating before model refresh | `notebooks/nb_data_quality.py` |
| T-SQL warehouse marts, procs, window functions | `warehouse/finance_marts.sql` |
| Direct Lake semantic model: calc groups, RLS, field parameters | `semantic-model/model_design.md` |
| Environment-parameterized CI/CD with fabric-cicd | `cicd/deploy.py`, `cicd/parameter.yml` |
| Real-time intelligence (KQL, update policies) — planned | `eventhouse/kql_setup.kql` |
| Engineering judgment | `DECISIONS.md` |

## Repository layout

| Path | What it is |
|---|---|
| `/notebooks`, `/pipelines`, `/warehouse`, `/semantic-model` | Authored source and design documentation |
| `/workspace` | Live Fabric item definitions, synced by Git integration from NKF-Dev |
| `/cicd` | fabric-cicd deployment script (`deploy.py`) and environment parameter file (`parameter.yml`) |
| `/data-generator` | Synthetic data generators (batch + streaming) |
| `DECISIONS.md` | Architecture decision log |

## Deploying to an environment

The deployment script authenticates with an Azure service principal (Contributor on each target workspace) via environment variables, then publishes the in-scope items with the correct per-environment substitutions applied.

```powershell
$env:AZURE_TENANT_ID   = "<tenant id>"
$env:AZURE_CLIENT_ID   = "<client id>"
$env:AZURE_CLIENT_SECRET = "<secret>"

python cicd/deploy.py --environment test --scope full
```

`--environment` selects the target (`test` or `prod`); `--scope` selects the item set (`core` or `full`).

## Bootstrapping a new environment

Because lakehouses and warehouses are provisioned by hand rather than deployed, a fresh environment needs a one-time setup before its first pipeline run. This is deliberate — data containers are environment infrastructure, not versioned logic — but it means the sequence has to be followed:

1. Create the workspace lakehouse (`lh_freight`) and warehouse (`wh_freight`). Enable lakehouse schemas — see the lesson on schema consistency below.
2. Run `pipelines/00_control_table.sql` against the warehouse to create and populate `etl.ingestion_control`.
3. Land the source CSVs under the lakehouse's `Files/landing/` in the expected folder layout.
4. Grant the deployment service principal Contributor on the workspace (Viewer is not sufficient — it does not grant OneLake data access).
5. Deploy with `--scope full`, then run the ingestion pipeline with `IsFullReload = true`.
6. Bind the semantic model's Direct Lake connection and refresh.

## Lessons learned

The batch platform works, but getting the three environments to behave identically surfaced a class of problem worth documenting, because it is the kind of thing that does not appear in a tutorial.

**Environment drift is invisible until it isn't.** The three workspaces were provisioned by hand on different days. One lakehouse was created without schemas enabled and the other two with schemas, which changes where tables physically live (`Tables/gold_dim_carriers` versus `Tables/dbo/gold_dim_carriers`). A Direct Lake model that references tables without a schema name resolves in one environment and fails in the other — and Fabric reports the failure as "one or more source tables either do not exist or access was denied," which sends you chasing permissions for hours. The fix was to make the table partitions schema-qualified and standardize schemas across environments. The deeper fix, on the roadmap, is to provision lakehouses and warehouses from a script so the drift cannot happen.

**A deployment pipeline moves logic, not containers.** fabric-cicd faithfully deploys item definitions. It does not create the lakehouses, warehouses, control tables, or landing data those items depend on. That separation is correct, but it means every environment needs an explicit bootstrap step, which is why the sequence above exists.

**Service principal permissions are more granular than they look.** A workspace Viewer role grants read through the SQL endpoint but not direct OneLake data access, so a Copy activity reading files through a shortcut fails with a permission error even though the identity can query the same data in SQL. Contributor is the minimum for pipeline-driven ingestion.

## Roadmap

- **Provision infrastructure as code** — lakehouse and warehouse creation from a script, closing the drift gap that caused the schema issue above.
- **Prod deployment guard** — require an explicit `--confirm-prod` flag, since the deploy removes orphaned items on the target.
- **Secret management** — move the service principal secret out of environment variables into Key Vault.
- **Streaming layer** — Eventstream, Eventhouse with update policies and materialized views, Activator alerting, and a Real-Time Dashboard.

## Demos (planned)

1. Metadata-driven onboarding: add one control-table row, a new source flows to Bronze with zero pipeline edits.
2. Backfill and targeted rerun via pipeline parameters.
3. Feature branch to Prod: a change deploys to Test on merge and to Prod on tag after approval.
4. Live reefer excursion: the simulator fires a temperature spike and an Activator alert lands within a minute.
