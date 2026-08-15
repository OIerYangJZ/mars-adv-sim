# MOSAIC-Ω V3 前六项任务实施与验收记录

## 总原则

本版本按《项目推进与负责人执行手册》收口为一条权威主链。验收口径区分：

- **实现完成**：代码、接口、部署脚本、自动化测试均已存在；
- **本地通过**：当前无外部服务环境可重复运行；
- **生产待实机**：需要 Docker / PostgreSQL / Redis / Mosquitto / OR-Tools / ROS 2 的项目，当前沙箱没有这些运行条件，因此不伪造实机结果。

## 任务 1：PostgreSQL EventStore + Redis Memory + Docker Compose

### 实现

- PostgreSQL：`execution_scheduler.adapters.postgres.PostgresDatabase`
- append-only EventStore + projection + outbox + snapshot + idempotency
- Redis：`memory_recovery.adapters.redis_store.RedisStore`
- Redis Memory repository/index
- 共享文件对象存储：`storage.LocalObjectStore`
- 生产装配：`integration.production.build_production_chain()`
- Docker Compose：PostgreSQL 16、Redis 7、Mosquitto 2；Smoke profile 运行主应用

### 自动验收

```bash
./scripts/smoke_production.sh
```

成功条件：PostgreSQL/Redis health=true、E2E all_succeeded=true、实际 Scheduler policy **只能是 `ortools`**。

### 当前环境结论

代码与部署路径完成；当前沙箱没有 Docker/psycopg/redis 服务，生产 Smoke 未在本环境执行。

## 任务 2：OR-Tools Min-Cost Flow + Capability Posterior

### 实现

- `BetaPosteriorUpdater`：成功/失败/质量/timeout 更新；指数衰减旧样本。
- `CostModel`：posterior reliability 直接进入 failure cost。
- `Scheduler._ortools()`：Task→Device 最小费用流；容量约束；硬权限/隐私/位置过滤先于优化。
- Production `SCHEDULER_ALLOW_FALLBACK=false`，OR-Tools 不可用即失败。
- Greedy / Round-Robin 仅保留为基线和显式 demo fallback。

### 本地工程基准

当前沙箱无 OR-Tools，因此不声明 OR-Tools 指标。100 Task × 20 Resource 的 Greedy 工程基准：

- P95：约 177 ms
- 100/100 assignments

Event 内存基准 100000 events：

- append P95：约 0.081 ms
- replay：约 8.50 s

这些仅是本地工程基准，不替代 PostgreSQL/OR-Tools 生产指标。

## 任务 3：真实 MQTT Agent 接入权威主链

### 实现

- `PahoMqttRpcClient`：QoS1 request/reply，correlation_id，timeout。
- `MqttAgentAdapter`：远程 Agent 只负责生成 ToolCall。
- `PahoPlannerWorker`：独立 Agent worker。
- `MosaicMainChain.register_mqtt_agent()`：通过同一 CapabilityRegistry 注册远端 Agent。
- 权威 ToolRuntime 仍在 Orchestrator 侧执行，远端 Agent 不能绕过权限、幂等、Evidence、EventStore、Verifier。

### 自动验收

```bash
./scripts/smoke_mqtt.sh
```

成功条件：任务必须实际分配给 `mqtt-agent-1`，PostgreSQL/Redis healthy，最终 verified，实际 scheduler policy=`ortools`。

### 当前环境结论

协议与主链接口已由 Fake RPC 单测通过；当前沙箱无 Mosquitto/paho，真实 Broker Smoke 待 Docker 环境执行。

## 任务 4：Verifier + Evidence Manifest

### 实现

- `VerifierService` 与 Agent/Executor 分离。
- Predicate DSL：execution_success、contains、file_exists、file_contains。
- 每个 Tool Evidence 有实际文件工件和 SHA-256。
- Verifier 重新读取工件验证哈希；缺 Evidence 或篡改均不能成功。
- `Evidence Manifest` 包含 uri/hash/producer/node/time/verifier/status。

### 当前结论

本地自动测试通过；ROS Repair 6 个 Task 均生成 Verified Evidence。

## 任务 5：Recovery Engine / Impact Graph

### 实现

影响边包括：

- Task `depends_on` 执行依赖；
- `evidence_dependencies` 证据依赖。

动作全部进入 EventStore：

- RETRY：只重试失败节点；
- REPLACE：下线失败 Actor 后重新调度；
- ROLLBACK：只执行任务显式声明的 `rollback_tool`；无补偿规则则 Safe Stop；
- REPLAN：仅重置影响闭包，未受影响已完成节点保持 SUCCEEDED；
- SAFE_STOP：PAUSED，并记录 `SAFE_STOP_TRIGGERED`。

Evidence 失效可通过 `MosaicMainChain.invalidate_evidence()` 触发 Memory invalidation + execution impact replan。

### 当前结论

Retry、Replan、Rollback、Safe Stop、Evidence invalidation 均有自动测试并通过。

## 任务 6：ROS Repair 场景

### 真实主链

```text
UserGoal
→ GoalSpec
→ ROS Repair TaskGraph
→ Capability/Scheduler
→ ToolRuntime
→ Evidence
→ Verifier
→ Report
```

没有第二个 scenario orchestrator；场景只提供 TaskGraph 与 Tool metadata。

### 场景步骤

1. inventory：读取 ROS 2 `ament_python` package.xml/目录；
2. diagnose：运行 pytest，保存失败输出和根因；
3. patch：最小修改并保存 patch evidence；
4. build：ROS 2 环境使用 `colcon build --packages-select demo_ros_pkg`；
5. test：pytest 回归；
6. report：根因、Patch、Build mode、最终测试、复现步骤。

当前沙箱没有 colcon，因此 demo 明确记录 `compileall_ci_fallback`。设置 `MOSAIC_ROS_REQUIRE_COLCON=1` 后，没有 colcon 会直接失败，禁止伪装生产构建成功。

### 当前本地结果

- 6/6 Task SUCCEEDED
- 6 Evidence
- 6 VerificationResult
- `repair_report.md` 生成
- `evidence_manifest.json` 生成

## 代码去冗余结果

- 删除第二套 `scheduler/`。
- 删除旧原型中的第二套执行状态链。
- 删除重复 planner-runtime bridge。
- 删除 package 内过期/不可运行的旧测试与 mock fixture，迁移到统一 `tests/`。
- 删除 Memory 未使用的第二套 EvidenceRef DTO。
- Execution Event 与 Memory 内部投影明确区分为 `Event` / `MemoryEvent`。
- 两套本地 ObjectStore 合并为一个 `storage.LocalObjectStore`。
- 场景不实现自己的 Orchestrator、Scheduler、Verifier、Recovery。
- 新增架构守卫测试，阻止第二套 Scheduler/EventStore/ToolRuntime/Verifier/Recovery/Memory/MainChain 或旧运行路径重新进入主仓。

## 最终本地回归

```text
54 passed
mainchain demo: 6/6 succeeded
ROS repair: 6/6 succeeded
compileall: PASS
```
