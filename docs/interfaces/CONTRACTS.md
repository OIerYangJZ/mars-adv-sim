# Interface Contracts

所有跨模块结构以 `src/mosaic_omega/integration/contracts.py` 和 `execution_scheduler.models` 为准。

## Planner → Execution

Planner 输出 TaskGraph；`ExecutionSchedulerService._task()` 是唯一 TaskNode 适配边界，负责提升：

- required_capabilities / required_permissions
- inputs / outputs
- predecessors
- evidence_dependencies
- resource_requirements
- risk / privacy / latency
- acceptance

场景代码不得自行维护第二种 Runtime Task 状态。

## Registry → Scheduler

`runtime.models.AgentProfile` 只描述外部 Agent 广告；`DynamicRegistrySchedulerBridge` 将其转换成调度侧 `CapabilityProfile`。在线状态、负载、时延通过同一个 CapabilityRegistry 更新。

## MQTT Agent → ToolRuntime

远程 Agent 只通过 `PLAN_REQUEST/PLAN_RESPONSE` 返回 ToolCall 计划。Orchestrator 会覆盖 run/task/actor/trace/model/schema 字段，随后交给权威 ToolRuntime 执行。因此 MQTT 不绕过：权限、幂等、证据、Event 和 Verifier。

## Execution → Memory

权威 Execution Event 通过 `execution_event_to_memory()` 投影成 `MemoryEvent`。Memory 内部 DTO 不作为第二个执行 Event 事实源。

## Evidence → Verification

Verifier 必须同时满足：

1. ExecutionResult 成功；
2. 至少一个 Evidence；
3. Evidence SHA-256 与实际工件一致；
4. Task acceptance predicates 全部通过。

否则不得进入 SUCCEEDED。
