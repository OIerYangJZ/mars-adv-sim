# MOSAIC-Ω V3 Observability Console Acceptance

## Scope

This acceptance covers only the core-platform observability layer. ROS Repair business logic is unchanged.

## Architecture constraints

- EventStore remains the authoritative execution fact source.
- Console is read-only and consumes atomic observability projections.
- Console does not import or instantiate EventStore, Scheduler, ToolRuntime, RecoveryEngine or VerifierService.
- Memory data is obtained through `MemoryService.observability_records`; Console never accesses Redis directly.
- No external CDN / JavaScript framework is required by the frontend.
- Token cards report `INSUFFICIENT DATA` unless real model/tokenizer usage is present.

## Local verification

```text
pytest: 59 passed
compileall: PASS
main-chain demo: 6/6 SUCCEEDED
ROS Repair regression: 6/6 SUCCEEDED (existing local fallback behavior unchanged)
```

HTTP smoke:

```text
GET  /api/health      -> 200 {ready: true, read_only: true}
GET  /api/task-graph  -> 200, 6 nodes / 6 edges
POST /api/snapshot    -> 405 read_only_console
```

Live console smoke observed the following projection phases:

```text
RUNNING / context_prepared
RUNNING / round_complete
SUCCEEDED / round_complete
SUCCEEDED / run_complete
```

## Visualized core views

- Overview KPI and task status distribution
- Dynamic TaskGraph
- Dynamic sparse topology / λ2 or connectivity
- Receiver-conditioned communication policy decisions
- Scheduler Assignment + cost breakdown + CapabilityProfile
- Working/Episodic/Semantic/Procedural memory projection
- ContextPack compression metrics
- Recovery Event timeline and affected-node closure
- Evidence Manifest and VerificationResult
- Unified Event / Trace timeline

## Runtime files

Per workspace:

```text
observability/latest.json
observability/runs/<run_id>.json
observability/logs/events.jsonl
```

These are disposable projections. Deleting them does not change authoritative runtime state.
