# Interface contracts

## GoalSpec

Top-level keys are frozen to exactly six fields. See `src/mosaic_omega/schemas/goalspec.schema.json`.

## Planner → Runtime

`ToDAGEngine.coordinator_plan()` returns Coordinator-compatible nodes. `plan_to_task_specs()` is the only supported conversion into Runtime `TaskSpec`.

Rich metadata carried through TaskSpec includes:

- hard_constraints
- soft_preferences
- prohibitions
- acceptance_conditions / acceptance_predicates
- evidence_dependencies
- budget
- risk
- resource_requirements
- source_refs

## Runtime context bootstrap

`TaskContextStore.initialize_from_spec()` maps hard constraints/prohibitions to lossless `ConstraintDelta`s and other planning metadata to facts before receiver-conditioned communication starts.

## Replanning

Use full latest plan + ToDAG `change_set`. Runtime preserves unaffected TaskRecord state and resets affected nodes to pending.
