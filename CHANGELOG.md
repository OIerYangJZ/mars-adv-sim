# Changelog

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
