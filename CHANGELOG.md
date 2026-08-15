# Changelog

## 0.2.0 - Core observability console

- Added a read-only, self-contained web console for the authoritative main chain.
- Added TaskGraph, dynamic topology, low-entropy communication, scheduler, memory, recovery, evidence and Event Trace views.
- Added JSONL structured event projection and central derived MetricRegistry.
- Added atomic per-run dashboard snapshots under the runtime workspace.
- Added architecture guard preventing Console from importing runtime mutation authorities.
- Added Docker Compose `console` profile with read-only workspace mount.
- Added 5 observability/architecture tests; total local suite is 59 tests.
- Kept ROS Repair and scenario code isolated from the core Console.

## 0.1.0 - integrated core

- Merge GoalSpec Compiler, ToDAG and receiver-conditioned dynamic runtime into one `src/` monorepo.
- Remove duplicate embedded runtime from the old DAG project.
- Fix GoalSpec JSON Schema drift and enforce exact six-field root contract.
- Improve rule fallback clause splitting and must-not classification.
- Move Planner → Runtime conversion to `integration/planner_runtime_bridge.py`.
- Restore resource scheduler → Coordinator placement adapter.
- Propagate ToDAG hard constraints, prohibitions, preferences, acceptance conditions, budget, risk and evidence dependencies into runtime TaskContext.
- Add planner change-set synchronization that preserves unaffected runtime state.
- Add GitHub Actions CI, modern `pyproject.toml`, `.gitignore`, unified README and integration tests.
