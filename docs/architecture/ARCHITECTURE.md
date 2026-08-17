# MOSAIC-Ω V3 Architecture Rules

## 1. One authority per concern

The runtime is a modular monolith at this stage: logical services are separated by package/interface, while a single process may host them for reliability and low integration cost.

- Planner owns intent and TaskGraph, never live execution state.
- EventStore owns Task execution state and replay.
- CapabilityRegistry owns current actor profile; BetaPosteriorUpdater owns learned reliability.
- Scheduler owns placement; no adapter performs its own scheduling.
- ToolRuntime is the only side-effect boundary.
- Verifier alone may approve a result as successful.
- RecoveryEngine computes impact and recovery action; EventStore performs the state mutation.
- MemoryService is the only public memory API; Redis is an adapter, not a fact authority for execution state.
- MessageTopologyService owns topology/receiver-conditioned communication only.
- Integration code converts contracts and sequences services; it contains no duplicate domain algorithm.

## 2. Production facts and caches

- PostgreSQL: authoritative execution Event log, Task projection, capability profiles, idempotency and outbox.
- Redis: memory records, working-memory cache/indexes.
- Workspace file store: snapshots, tool evidence and deliverables.
- Vector index: recall accelerator only; deleting it must not destroy authoritative memory records.

## 3. No duplicate runtime paths

There is one EventStore-owned task lifecycle, one scheduler package, and no scenario-specific orchestrator. Scenario plugins submit a TaskGraph to `MosaicMainChain.run_plan()` and use the same execution chain.
