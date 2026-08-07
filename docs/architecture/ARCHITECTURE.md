# Architecture

## Ownership

- `goal_planner.goalspec`: only requirement compilation.
- `goal_planner.todag`: only planning structure and local replanning.
- `runtime`: owns live assignment/communication state.
- `scheduler`: owns resource selection algorithms/adapters.
- `integration`: translates contracts; must not contain domain algorithms.

## Runtime state rule

ToDAG describes what should be executed. Runtime describes what is actually executing.
For a live deployment, EventStore/Orchestrator will become the source of truth for execution state; ToDAG runtime fields remain useful for planner-local simulation and tests only.
