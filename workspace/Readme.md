# Workspace definitions (Fabric Git integration)

This folder is **generated and maintained by Microsoft Fabric**, not by hand.
The `NKF-Dev` workspace is connected to this repository, and every commit here
is an export of the live item definitions: pipelines, notebooks, the semantic
model (TMDL), the report, and lakehouse/warehouse metadata.

Two consequences worth knowing:

- **Do not edit these files directly.** Changes are made in Fabric and committed
  from the workspace's Source control pane. Editing here risks conflicts on the
  next sync.
- **This is the deployment source.** `cicd/deploy.py` uses `fabric-cicd` to
  publish these definitions to the Test and Prod workspaces, rebinding
  environment-specific IDs via `cicd/parameter.yml`.

The authored, documented source — with commentary and design intent — lives in
`/notebooks`, `/pipelines`, `/warehouse`, and `/semantic-model` at the repo root.
Read those to understand the platform; read this folder to see exactly what is
deployed.
