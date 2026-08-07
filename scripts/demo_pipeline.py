#!/usr/bin/env python3
from __future__ import annotations

import json

from mosaic_omega.integration.pipeline import compile_runtime_plan
from mosaic_omega.runtime.task_context import TaskContextStore


def main() -> None:
    user_text = "开发一个ROS2系统，不能上传用户数据，必须通过测试，尽量减少改动"
    compiled = compile_runtime_plan(user_text, goalspec_mode="rule", planning_horizon=6)

    contexts = TaskContextStore()
    for spec in compiled.task_specs:
        contexts.initialize_from_spec(spec, replace=True)

    first = compiled.task_specs[0]
    print("=== GoalSpec ===")
    print(json.dumps(compiled.goalspec, ensure_ascii=False, indent=2))
    print("\n=== TaskGraph summary ===")
    print(json.dumps({
        "revision": compiled.taskgraph["revision"],
        "node_count": compiled.taskgraph["graph_metrics"]["node_count"],
        "edge_count": compiled.taskgraph["graph_metrics"]["edge_count"],
        "ready_task_ids": compiled.taskgraph["ready_task_ids"],
    }, ensure_ascii=False, indent=2))
    print("\n=== First runtime TaskSpec ===")
    print(json.dumps(first.to_dict(), ensure_ascii=False, indent=2))
    print("\n=== Bootstrapped TaskContext ===")
    print(json.dumps(contexts.snapshot(first.task_id), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
