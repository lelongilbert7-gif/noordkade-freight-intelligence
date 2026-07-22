"""
Deploy Noordkade Freight workspace items to Test or Prod.

Authentication: fabric-cicd uses azure-identity's DefaultAzureCredential, which
picks up AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET from the
environment. GitHub Actions supplies those from repository secrets; locally you
can export the same three variables to run a deployment by hand.

Usage:
    python cicd/deploy.py --environment test
    python cicd/deploy.py --environment prod
    python cicd/deploy.py --environment test --dry-run
    python cicd/deploy.py --environment test --scope core
"""

import argparse
import os
import sys
from pathlib import Path

from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
)

WORKSPACE_IDS = {
    "test": "6e368542-3852-4a02-9ed2-117c53d63afa",
    "prod": "177064c9-bb7e-41bd-a29e-96325856b81d",
}

# Staged rollout. "core" proves the loop with the items that carry the logic;
# "full" adds the analytics layer once core is green. Lakehouse and Warehouse
# are deliberately excluded: they are created once per environment and hold
# data, so they are not redeployed on every merge.
SCOPES = {
    "core": ["Notebook", "DataPipeline"],
    "full": ["Notebook", "DataPipeline", "SemanticModel", "Report"],
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--environment", required=True, choices=["test", "prod"])
    ap.add_argument("--scope", default="full", choices=["core", "full"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    workspace_dir = repo_root / "workspace"

    if not workspace_dir.exists():
        sys.exit(
            f"workspace/ not found at {workspace_dir}. "
            "NKF-Dev must be connected to Git and committed first."
        )

    item_count = len(list(workspace_dir.glob("*/.platform")))
    print(f"Found {item_count} items in {workspace_dir}")
    print(f"Target: {args.environment} ({WORKSPACE_IDS[args.environment]})")
    print(f"Scope:  {args.scope} -> {SCOPES[args.scope]}")

    if args.dry_run:
        missing = [
            v for v in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_CLIENT_SECRET")
            if not os.environ.get(v)
        ]
        print(f"[dry-run] Credentials present: {'no — ' + ', '.join(missing) if missing else 'yes'}")
        print("[dry-run] No changes published.")
        return

    workspace = FabricWorkspace(
        workspace_id=WORKSPACE_IDS[args.environment],
        environment=args.environment,
        repository_directory=str(workspace_dir),
        item_type_in_scope=SCOPES[args.scope],
    )

    publish_all_items(workspace)

    # Items deleted from the repo are removed from the target, so environments
    # never drift away from what is in source control.
    unpublish_all_orphan_items(workspace)

    print(f"Deployment to {args.environment} complete.")


if __name__ == "__main__":
    main()
