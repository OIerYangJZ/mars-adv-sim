# MOSAIC-Ω V3 Core Monorepo

这是把原来的三个原型工程整理后的统一仓库：

1. GoalSpec Compiler：自然语言 → 固定六字段 GoalSpec。
2. ToDAG：GoalSpec → 动态因果 TaskGraph，支持局部重规划。
3. Dynamic Runtime：Agent 注册、任务路由、动态拓扑、receiver-conditioned 低熵通信。
4. Scheduler Adapter：把现有端/边/云资源评分器接入 Coordinator 的 placement port。
5. Integration：只负责模块间转换与同步，不复制算法实现。

## 当前已打通的主链

```text
Natural language
   ↓
GoalSpec Compiler
   ↓  six frozen root fields
ToDAGEngine
   ↓  coordinator_plan()
PlannerRuntimeBridge
   ↓  TaskSpec
Coordinator / TaskStore
   ↓
TaskContextStore.initialize_from_spec()
   ↓
Receiver-conditioned low-entropy communication
```

本次整理修复了三个原工程之间的几个关键问题：

- GoalSpec JSON Schema 与编译器真实输出不一致。
- ToDAG 中重复内嵌旧版 `dynamic_registry_sim`。
- 新版低熵 Runtime 缺少 ToDAG bridge 与 Scheduler adapter。
- ToDAG 的 `hard_constraints / prohibitions / acceptance / risk / budget` 只进入 TaskSpec metadata，没有进入 TaskContext。
- ToDAG 局部重规划的 `change_set` 无法同步 Runtime；现在 `TaskStore.apply_planner_update()` / `Coordinator.apply_planner_update()` 会保留未受影响节点并重置受影响节点。
- 规则版 GoalSpec 对逗号分隔约束的粒度过粗；现在按子句抽取，并把“不得/不能/禁止”同时视作 hard constraint + prohibition。

## 项目结构

```text
mosaic-omega/
├── src/mosaic_omega/
│   ├── goal_planner/
│   │   ├── goalspec/          # GoalSpec compiler
│   │   └── todag/             # dynamic causal TaskGraph
│   ├── runtime/               # registry, coordinator, low-entropy messaging
│   ├── scheduler/             # resource scheduler + runtime adapter
│   ├── integration/           # planner/runtime bridge and sync
│   └── schemas/               # shared frozen contracts
├── tests/
│   ├── unit/
│   └── integration/
├── experiments/benchmarks/
├── docs/
├── scripts/
└── .github/workflows/
```

## 安装

建议 Python 3.10+。

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e ".[dev]"
```

需要 DeepSeek：

```bash
pip install -e ".[dev,deepseek]"
export DEEPSEEK_API_KEY="..."
```

需要 MQTT：

```bash
pip install -e ".[dev,mqtt]"
```

## 测试

```bash
pytest
```

重点集成测试：

```bash
pytest tests/integration/test_goal_to_runtime.py
pytest tests/integration/test_replan_runtime_sync.py
pytest tests/integration/test_scheduler_adapter.py
```

## Demo

```bash
python scripts/demo_pipeline.py
```

Demo 会执行：

```text
自然语言
→ GoalSpec
→ ToDAG
→ TaskSpec
→ TaskContext 初始化
→ 打印约束是否成功贯通
```

## 关键接口约束

GoalSpec 顶层字段禁止改名、增加或删除：

```text
main_goal
hard_constraints
soft_preferences
acceptance_conditions
budget
prohibitions
```

跨模块不要直接复制内部数据结构。Planner → Runtime 统一走：

```python
from mosaic_omega.integration.planner_runtime_bridge import plan_to_task_specs
```

动态需求变化统一走：

```python
await coordinator.apply_planner_update(task_specs, snapshot["change_set"])
```

## 目前边界

这次仓库整理的目标是把你已有的三个代码包真正合并并收口接口，不等于报告中所有模块已经完成。以下仍属于后续模块：

- PostgreSQL EventStore / 可重放执行内核
- ToolRuntime 沙箱与幂等副作用
- Beta Capability posterior
- OR-Tools 批量最小费用流正式调度器（当前 `ResourceScheduler` 是现有加权评分 baseline）
- 谱约束动态拓扑升级
- Redis 分层 Memory
- Verifier / Evidence Manifest / Recovery Engine

这些后续可以继续按当前 package 边界增加，不需要再拆仓库。
