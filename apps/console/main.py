#!/usr/bin/env python3
"""Launch the MOSAIC-Ω read-only operator console."""
from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from mosaic_omega.integration import MosaicMainChain

from backend.server import serve


def _run_demo(workspace: Path, goal: str, run_id: str) -> None:
    # Give the browser a moment to connect; snapshots are then refreshed by the
    # authoritative main chain at each integration phase.
    time.sleep(0.4)
    chain = MosaicMainChain(workspace=workspace)
    chain.run(goal, run_id=run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description="MOSAIC-Ω core operator console")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--workspace", default=".mosaic_workspace/console")
    parser.add_argument("--snapshot-dir", default=None)
    parser.add_argument("--demo", action="store_true", help="run an in-process demo into the same read-only console")
    parser.add_argument("--run-id", default="console-demo")
    parser.add_argument("--goal", default="修复 ROS 仓库，必须通过测试，不得修改公共接口。")
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    snapshot_dir = Path(args.snapshot_dir).resolve() if args.snapshot_dir else workspace / "observability"
    frontend_dir = Path(__file__).resolve().parent / "frontend"

    if args.demo:
        thread = threading.Thread(
            target=_run_demo,
            args=(workspace, args.goal, args.run_id),
            name="mosaic-console-demo",
            daemon=True,
        )
        thread.start()

    serve(args.host, args.port, snapshot_dir=snapshot_dir, frontend_dir=frontend_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
