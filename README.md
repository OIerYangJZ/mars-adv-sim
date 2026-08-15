# MOSAIC-Ω V3 — Authoritative Main Chain

本仓库是 MOSAIC-Ω V3 的**唯一运行主仓**。原 `mars-adv-sim-main`、`dynamic_registry_sim_v4`、Memory 与 Execution Scheduler 原型中的有效实现已收口到这里；不存在第二套 Task 状态机、第二套 Scheduler 或第二套 Orchestrator。

## 1. 唯一主链

```text
UserGoal
  ↓
GoalSpec Compiler
  ↓
ToDAG / TaskGraph
  ↓
Capability Registry + Beta Posterior
  ↓
OR-Tools Min-Cost Flow Scheduler
  ↓
Memory Context + Dynamic Topology + Low-Entropy Message
  ↓
Agent Adapter (in-process / MQTT)
  ↓
ToolRuntime  ← 唯一外部工具执行入口
  ↓
ExecutionResult + Evidence
  ↓
Verifier  ← 执行与验收分离
  ↓
SUCCEEDED / RecoveryEngine
  ↓
EventStore + Memory sync + Evidence Manifest
```

### 单一职责 / 单一事实源

| 关注点 | 唯一所有者 |
|---|---|
| 目标与硬约束 | `goal_planner.goalspec` |
| TaskGraph | `goal_planner.todag` |
| 执行状态 / Event / Replay | `execution_scheduler.EventStore` |
| 能力画像 / 后验 / 分配 | `execution_scheduler` |
| 外部工具副作用 | `execution_scheduler.ToolRuntime` |
| 动态拓扑 / 低熵通信 | `runtime.message_topology` |
| 长程记忆 | `memory_recovery.MemoryService` |
| 证据验收 | `verifier.VerifierService` |
| 影响子图 / 恢复 | `recovery.RecoveryEngine` |
| 文件对象存储 | `storage.LocalObjectStore` |
| 跨模块装配 | `integration.MosaicMainChain` |

`integration` 只编排和转换接口，不复制算法。

## 2. 已落地的前六项任务

1. PostgreSQL EventStore + Redis Memory + Docker Compose。
2. Beta Capability Posterior + OR-Tools Min-Cost Flow；生产禁止静默 fallback。
3. MQTT Remote Agent 接入权威主链；远端只产生 `ToolCall`，副作用仍由本地 `ToolRuntime` 执行。
4. 独立 Verifier + SHA-256 Evidence + Evidence Manifest。
5. Impact Graph + Retry / Replace / Rollback / Replan / Safe Stop。
6. ROS 2 `ament_python` 修复场景：盘点 → 诊断 → Patch → Build → pytest → 报告。

## 3. 安装

### 本地开发 / CI

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e ".[dev]"
```

### 生产依赖

```bash
pip install -e ".[all]"
cp .env.example .env
```

`.[all]` 包含 PostgreSQL、Redis、OR-Tools 和 MQTT 客户端依赖。

## 4. 本地确定性验收

```bash
./scripts/verify_local.sh
```

该命令依次执行 compileall、全量测试、主链 Demo 和 ROS Repair Demo。

本地 demo 使用 InMemory EventStore/Memory 以保证无外部服务时也能复现接口与状态逻辑；生产路径不会把这一后端当成正式事实源。

## 5. 生产后端验收

### PostgreSQL + Redis + OR-Tools

```bash
./scripts/smoke_production.sh
```

该命令在 Docker 内运行真实 PostgreSQL、Redis，并要求 Assignment 的实际策略必须是 `ortools`。如果 OR-Tools 或数据库连接不可用，验收直接失败，不允许静默降级。

### MQTT 真实 Agent

```bash
./scripts/smoke_mqtt.sh
```

也可以连续执行任务 1～3 的生产验收：

```bash
./scripts/verify_production.sh
```

该命令启动 Mosquitto、远程 Agent Worker 与权威 Orchestrator；最终任务必须由 `mqtt-agent-1` 规划，并继续经过同一个 Scheduler、ToolRuntime、Evidence、Verifier 和 EventStore。

## 6. ROS Repair 场景

```bash
PYTHONPATH=src:. python scripts/demo_ros_repair.py
```

场景仓库是标准 ROS 2 `ament_python` 结构。ROS 2 机器上自动调用：

```text
colcon build --packages-select demo_ros_pkg
pytest
```

通用 CI 如果没有 `colcon`，会明确记录 `compileall_ci_fallback`，不会伪装成 colcon 成功。生产/答辩环境可设置：

```bash
export MOSAIC_ROS_REQUIRE_COLCON=1
```

此时无 `colcon` 会直接失败。

## 7. 关键目录

```text
src/mosaic_omega/
├── goal_planner/
├── execution_scheduler/
├── runtime/message_topology/
├── memory_recovery/
├── verifier/
├── recovery/
├── integration/
└── storage.py

scenarios/ros_repair/
tests/unit/
tests/integration/
deploy/docker-compose.yml
scripts/
```

详细架构与手册映射见 `docs/architecture/MAIN_CHAIN.md`；六项验收记录见 `docs/reports/TASKS_1_6_ACCEPTANCE.md`。

## 核心运行可视化 Console

核心平台内置只读 Observability Console，用同一套权威主链数据展示 TaskGraph、动态拓扑、低熵通信决策、Scheduler Assignment、Memory ContextPack、Recovery Timeline、Evidence/Verification 与统一 Event Trace，不创建第二套运行状态。

一键启动 Demo：

```bash
./scripts/demo_console.sh
```

浏览器打开 `http://127.0.0.1:8080`。查看已有运行：

```bash
PYTHONPATH=src:. python apps/console/main.py \
  --snapshot-dir .mosaic_workspace/mainchain-demo/observability
```

Docker 生产环境可用同一个只读界面：

```bash
docker compose -f deploy/docker-compose.yml --profile console up console
```

详细设计见 `docs/architecture/OBSERVABILITY_CONSOLE.md`；验收记录见 `docs/reports/OBSERVABILITY_CONSOLE_ACCEPTANCE.md`。
