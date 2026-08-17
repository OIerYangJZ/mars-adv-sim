# ToDAG 动态任务图编译层

`todag` 只负责 **GoalSpec → TaskGraph → execution plan**。它不维护运行期事实状态；任务状态、事件、Evidence 与 Replay 的唯一事实源是 `execution_scheduler.EventStore`。

## 1. 输入边界

ToDAG 接收冻结的六字段 GoalSpec：

```json
{
  "main_goal": {},
  "hard_constraints": [],
  "soft_preferences": [],
  "acceptance_conditions": [],
  "budget": {},
  "prohibitions": []
}
```

六个字段内部允许富结构对象，但禁止新增第七个顶层字段。若缺少 `acceptance_conditions`，图状态进入 `needs_clarification`，禁止导出执行计划。

## 2. TaskGraph 能力

- 任务依赖边区分 `exec / data / evidence / mutex`；
- 环检测、拓扑排序、关键路径、可执行节点与后继闭包；
- 默认滚动窗口 H=10，同时保留完整图用于重放和影响分析；
- `update_node()` 仅失效受影响闭包；
- `update_specification()` 增量重编译并保留未受影响节点结果；
- `invalidate_node()` 支持证据或前提失效后的局部重规划；
- 每个节点保存输入、输出、能力要求、资源要求、风险、成本、Evidence 依赖、验收谓词与回滚元数据。

`required_skill` 表示主能力，`required_skills` 表示完整能力集合，两者语义不同，不是两套调度接口。

## 3. 唯一执行边界

`ToDAGEngine.execution_plan()` 将 Planner 内部节点一次性投影为 Execution Scheduler 可接收的任务列表。之后不再存在第二套 Task 状态管理逻辑：

```text
GoalSpec
  ↓
ToDAGEngine.build()
  ↓ TaskGraph
ToDAGEngine.execution_plan()
  ↓
ExecutionSchedulerService.create_run()
  ↓
EventStore (authoritative state)
```

Planner-only 字段放在 `metadata`；Execution Scheduler 在 `_task()` 这一处完成边界提升，避免每个模块重复写适配逻辑。

## 4. CLI / API

```bash
PYTHONPATH=src python -m mosaic_omega.todag build goalspec.json \
  -o taskgraph.json \
  --execution-output execution-tasks.json
```

Web API：

- `POST /api/build`：构建 TaskGraph；
- `GET /api/dag`：TaskGraph 快照；
- `GET /api/ready`：ready nodes、滚动窗口与关键路径；
- `GET /api/execution-plan`：执行边界任务列表；
- `PATCH /api/nodes/{task_id}`：修改节点并局部失效；
- `PUT /api/specification`：更新 GoalSpec 并局部重规划；
- `POST /api/nodes/{task_id}/invalidate`：触发局部失效。

## 5. 测试

```bash
PYTHONPATH=src:. pytest -q tests/unit/todag
```

覆盖冻结 Schema、富 GoalSpec、局部重规划、Evidence 依赖、placement、环检测、无验收阻断、1000 节点图查询及 execution-plan 投影。
