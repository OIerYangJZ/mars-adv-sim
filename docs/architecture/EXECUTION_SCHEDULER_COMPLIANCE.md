# Execution Scheduler requirement compliance

This file maps the requested execution-scheduler package to the implementation.

| Requirement | Implementation | Verification |
|---|---|---|
| Execution-only data models | `execution_scheduler/models.py` | canonical-field tests |
| PostgreSQL + environment config | `config.py`, `adapters/postgres.py` | construction/schema tests |
| Append-only, Event-first, replay | `event_store.py` | replay/trace/snapshot tests |
| Strict lifecycle | `state_machine.py` | illegal-jump test |
| Sole ToolRuntime boundary | `tool_runtime.py` | permission/idempotency tests |
| Same side effect once | `idempotency.py` | repeat + fingerprint-conflict tests |
| Orchestrator does not contain scheduling algorithm | `orchestrator.py` | scheduler delegated separately |
| Persistent capabilities | `capability.py` | registry round-trip in tests/demo |
| Beta posterior + decay | `posterior.py` | end-to-end posterior update test |
| Explainable cost + hard filters | `cost_model.py` | privacy/location test |
| OR-Tools min-cost flow + baselines | `scheduler.py` | fallback test; OR-Tools path when dependency installed |
| Live resource refresh | `resource_monitor.py` | CPU/GPU/memory/queue/latency update test |
| Unified service facade | `service.py` | end-to-end DAG test |
| PostgreSQL transaction/outbox | `adapters/postgres.py` | same transaction in `append_bundle()` |
| Local executor sandbox boundary | `adapters/local_tool_executor.py` | POSIX/Windows path-escape test |
| Mock Agent without real LLM | `adapters/mock_agent.py` | end-to-end demo |

## Local verification result for this delivery

- `python -m execution_scheduler.run_demo`: all demo nodes reached `SUCCEEDED`.
- `python -m unittest discover -v`: 28/28 tests passed in the delivery environment.
- 100,000-event in-memory replay check: approximately 8.7 s; append P95 approximately 0.10 ms.
- 100-task x 20-resource in-memory greedy scheduling check: approximately 148 ms P95.

The last two numbers are local in-memory checks only. They do not substitute for
PostgreSQL/OR-Tools deployment benchmarks on the target machine.
