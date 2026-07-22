# Noordkade Freight: Operations Intelligence Platform

End-to-end Microsoft Fabric analytics platform for Noordkade Freight, a fictional Rotterdam-based 3PL. All data is synthetically generated; no real company data is used. The platform covers: batch shipment analytics through a medallion lakehouse and T-SQL warehouse, live fleet telematics through Eventstream and a KQL Eventhouse, a Direct Lake semantic model with calculation groups and RLS, and fully automated CI/CD across Dev, Test, and Prod with fabric-cicd and GitHub Actions.

Built by Gilbert Kiptoo (BluePeak Analytics). Certifications: DP-600, PL-300.

## Architecture

Batch: CSV landing (date-partitioned) → metadata-driven Data Pipeline → Bronze → Silver (PySpark merge, quarantine) → DQ gate → Gold star schema → Direct Lake semantic model → executive report.

Streaming: telematics simulator → Eventstream custom endpoint → Eventhouse (update policy shapes alerts at ingest) → Real-Time Dashboard + Activator rules (reefer temperature excursions, harsh braking, escalation on alert bursts).

Governance: three workspaces (Dev/Test/Prod), feature-branch Git workflow, PR validation, automated deployment to Test on merge, gated deployment to Prod on tag.

## What this demonstrates

| Capability | Where |
|---|---|
| Metadata-driven parameterized pipelines | `pipelines/PIPELINE_FRAMEWORK.md`, `pipelines/00_control_table.sql` |
| Incremental loads with self-healing watermarks | `notebooks/nb_silver_shipments.py` |
| Data quality gating before model refresh | `notebooks/nb_data_quality.py` |
| T-SQL warehouse marts, procs, window functions | `warehouse/finance_marts.sql` |
| Real-time intelligence (KQL, update policies, materialized views) | `eventhouse/kql_setup.kql` |
| Semantic model: calc groups, RLS, field parameters | `semantic-model/model_design.md` |
| Automated CI/CD with fabric-cicd | `cicd/` |
| Engineering judgment | `DECISIONS.md` |

## Repository layout

| Path | What it is |
|---|---|
| `/notebooks`, `/pipelines`, `/warehouse`, `/semantic-model` | Authored source and design documentation |
| `/workspace` | Live Fabric item definitions, synced by Git integration from NKF-Dev |
| `/cicd` | fabric-cicd deployment script, parameter file, GitHub Actions workflow |
| `/data-generator` | Synthetic data generators (batch + streaming) |
| `DECISIONS.md` | Architecture decision log |

## Build order (6-week evening plan, trial-capacity safe)

Week 1: run `data-generator/generate_batch_data.py`, upload to lakehouse Files/landing, create control table in the warehouse, build `pl_ingest_metadata_driven` per the framework doc.
Week 2: silver notebooks, quarantine, DQ gate, master orchestrator with backfill and TableFilter scenarios working.
Week 3: gold layer, warehouse marts, semantic model with calc group and RLS.
Week 4: executive report (4 pages), Test/Prod workspaces, connect Dev to Git.
Week 5: fabric-cicd pipeline live end to end, record the feature-branch-to-Prod Loom.
Week 6: Eventstream, Eventhouse, Activator, real-time dashboard, streaming Loom. Polish README, publish.

Rule for the trial: never run Spark jobs and streaming ingestion at the same time. Record the streaming demos in a window when no pipelines are scheduled.

## Demos (Loom)

1. Metadata-driven onboarding: add one control-table row, a new source flows to bronze with zero pipeline edits.
2. Backfill and targeted rerun via pipeline parameters.
3. Feature branch to Prod: DAX change → PR checks → merge deploys Test → tag deploys Prod after approval.
4. Live reefer excursion: simulator fires a temperature spike, Activator alert lands in email within a minute.
