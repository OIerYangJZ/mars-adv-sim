# MOSAIC-Ω V3 权威主链与手册接口映射

## 1. 在线主链

```text
UserGoal
  ↓
goal_planner.goalspec.compile_goal
  ↓ GoalSpec
goal_planner.todag.ToDAGEngine
  ↓ TaskGraph
execution_scheduler.CapabilityRegistry
  ↕ BetaPosteriorUpdater
execution_scheduler.Scheduler (OR-Tools min-cost flow)
  ↓ Assignment
memory_recovery.MemoryService + runtime.message_topology.MessageTopologyService
  ↓ ContextPack / MessageEnvelope
Agent Adapter (local / MQTT)
  ↓ ToolCall
execution_scheduler.ToolRuntime
  ↓ ExecutionResult / Evidence
verifier.VerifierService
  ↓ VerificationResult
recovery.RecoveryEngine (if failed)
  ↓
execution_scheduler.EventStore + Memory event sync
  ↓ Evidence Manifest
```

## 2. 权威边界

`TaskNodeView.state` 只能由 `EventStore` 的 event-first API 修改；Planner、MQTT Agent、Memory、Topology、Verifier 和场景插件均无权直接修改状态。

Agent 无权直接产生 `SUCCEEDED`。Agent 只返回 `ToolCall`；`ToolRuntime` 产生 `ExecutionResult/Evidence`；Verifier 根据可执行谓词和证据哈希决定是否通过。

## 3. 手册核心接口

### GoalSpec
`goal_id, objective, hard_constraints, soft_preferences, acceptance_predicates, budgets, privacy_level, source_spans`

### TaskNode
`node_id, type, inputs, outputs, predecessors, evidence_dependencies, resource_requirements, risk, status, acceptance`

### Event
`event_id, run_id, node_id, type, payload, timestamp, trace_id, parent_event_id, actor_id, model_id, schema_version`

Event append-only；状态投影由 Event 更新，可通过 Snapshot + Replay 重建。

### Evidence
`evidence_id, uri, hash, producer, node_id, mime_type, scope, created_at, verification_status`

Evidence 工件使用 SHA-256；Verifier 会重新读取 `file://` 工件验证哈希。

### CapabilityProfile
`actor_id, capabilities, reliability, cost, latency, context_limit, permissions, device_location, posterior`

动态 Agent 注册统一通过 `DynamicRegistrySchedulerBridge` 投影到 CapabilityRegistry；后验由执行结果更新。

### MessageEnvelope
`message_id, sender, receiver, topic, priority, ttl, budget, summary, evidence_refs, content_hash, causal_parent`

业务 MQTT 消息保持稳定 wire schema；额外 trace/hash/causal 元信息在主链边界统一投影。

### TopologySnapshot
`version, nodes, edges, edge_scores, lambda2_or_connectivity, effective_from, min_hold_time`

### VerificationResult
`target_id, passed, predicate_results, confidence, evidence_refs, risk_level, action`

## 4. 恢复语义

| ErrorClass | Action | 行为 |
|---|---|---|
| RETRYABLE | retry | 仅重置失败节点到 READY，受影响后继不重做 |
| REPLACEABLE | replace | 下线失败 Actor，再重新调度失败节点 |
| ROLLBACK_REQUIRED | rollback | 只执行任务显式声明的 `rollback_tool`，成功后局部重规划 |
| REPLAN_REQUIRED | replan | 根据 exec + evidence dependency 影响闭包重置子图 |
| SAFE_STOP | safe_stop | PAUSED 并记录安全停止事件，不猜测继续 |

恢复自身也写入 EventStore：`RECOVERY_PLANNED / ROLLBACK_EXECUTED / TASK_REPLANNED / SAFE_STOP_TRIGGERED`。

## 5. 生产后端

`build_production_chain()` 注入 PostgreSQL-backed `ExecutionSchedulerService` 与 Redis-backed `MemoryService`。生产 `.env` 固定：

```text
SCHEDULER_POLICY=ortools
SCHEDULER_ALLOW_FALLBACK=false
```

因此生产环境不会把 greedy fallback 伪装成最小费用流。
