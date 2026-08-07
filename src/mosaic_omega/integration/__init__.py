"""Cross-module adapters. Algorithms remain owned by their source modules."""

from .pipeline import CompiledRuntimePlan, compile_runtime_plan, sync_engine_to_runtime
from .planner_runtime_bridge import dag_to_task_specs, plan_to_task_specs

__all__ = [
    "CompiledRuntimePlan",
    "compile_runtime_plan",
    "sync_engine_to_runtime",
    "dag_to_task_specs",
    "plan_to_task_specs",
]
