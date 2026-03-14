#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv


def _resolve_dagster_home(repo_root: Path) -> Path:
    explicit_home = os.getenv("DAGSTER_HOME")
    if explicit_home:
        return Path(explicit_home).expanduser().resolve()

    home_hint = os.getenv("DAGSTER_HOME_DIR", ".dagster_home")
    home_path = Path(home_hint).expanduser()
    if not home_path.is_absolute():
        home_path = repo_root / home_path

    return home_path.resolve()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Dagster dev with configurable persistent DAGSTER_HOME."
    )
    parser.add_argument(
        "--workspace",
        default="workspace.yaml",
        help="Workspace file path relative to repo root (default: workspace.yaml).",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Only print resolved values and exit.",
    )
    parser.add_argument(
        "dagster_args",
        nargs="*",
        help="Extra args forwarded to `dagster dev`.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")

    dagster_home = _resolve_dagster_home(repo_root)
    dagster_home.mkdir(parents=True, exist_ok=True)
    os.environ["DAGSTER_HOME"] = str(dagster_home)

    workspace_path = (repo_root / args.workspace).resolve()

    cmd = [
        "dagster",
        "dev",
        "-w",
        str(workspace_path),
        *args.dagster_args,
    ]

    print(f"[run_dagster_dev] repo_root={repo_root}")
    print(f"[run_dagster_dev] DAGSTER_HOME={dagster_home}")
    print(f"[run_dagster_dev] command={' '.join(cmd)}")

    if args.print_only:
        return 0

    return subprocess.call(cmd, cwd=str(repo_root), env=os.environ.copy())


if __name__ == "__main__":
    sys.exit(main())
