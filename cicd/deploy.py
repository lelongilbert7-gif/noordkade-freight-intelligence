"""
fabric-cicd deployment script.
Deploys workspace items exported by Fabric Git integration (the /workspace folder)
into the target environment's workspace, rebinding item references via parameter.yml.

Usage:
  python cicd/deploy.py --environment test
  python cicd/deploy.py --environment prod
  python cicd/deploy.py --environment test --dry-run
"""

import argparse
import sys
from pathlib import Path

from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

WORKSPACE_IDS = {
    # Fill with your real workspace GUIDs (Settings > About this workspace)
    "test": "00000000-0000-0000-0000-00000000TEST",
    "prod": "00000000-0000-0000-0000-00000000PROD",
}

ITEM_TYPES = [
    "Notebook",
    "DataPipeline",
    "Lakehouse",
    "Warehouse",
    "SemanticModel",
    "Report",
    "Eventstream",
    "KQLDatabase",
    "VariableLibrary",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--environment", required=True, choices=["test", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    repo_dir = Path(__file__).resolve().parents[1] / "workspace"
    if not repo_dir.exists():
        print(f"workspace/ folder not found at {repo_dir}. "
              "Connect the Dev workspace to Git first so items export here.")
        sys.exit(1)

    ws = FabricWorkspace(
        workspace_id=WORKSPACE_IDS[args.environment],
        environment=args.environment,          # selects the block in parameter.yml
        repository_directory=str(repo_dir),
        item_type_in_scope=ITEM_TYPES,
    )

    if args.dry_run:
        print(f"[dry-run] Validated {sum(1 for _ in repo_dir.rglob('*.platform'))} "
              f"items against parameter.yml for '{args.environment}'.")
        return

    publish_all_items(ws)
    # Remove items deleted from the repo so environments never drift
    unpublish_all_orphan_items(ws)
    print(f"Deployed to {args.environment} ({WORKSPACE_IDS[args.environment]}).")


if __name__ == "__main__":
    main()
