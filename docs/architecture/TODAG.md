# ToDAG 动态任务图编译层

`todag` 位于 GoalSpec 与 Coordinator 之间，负责把固定六字段 GoalSpec 编译为可执行、可验证、可局部重规划的 TaskGraph。Coordinator 仍是运行期任务状态的唯一所有者。

## 冻结接口

GoalSpec 顶层只允许以下六个字段，字段名和数量均不改变：

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

六个字段内部既兼容旧版字符串，也兼容增强后的对象结构，例如 `main_goal.sub_goals`、约束 `predicate/source_span/confidence`、验收 `check_type/args` 等。任何第七个顶层字段都会被拒绝。

## 当前能力

1. GoalSpec 六字段强校验与富结构兼容。
2. 显式 `sub_goals` 优先；缺失时仅按验收条件做确定性最小分解，不凭空编造领域步骤。
3. 稳定语义 Task ID，需求追加时不导致无关节点整体换 ID。
4. TaskNode 保存输入、输出、能力、资源、风险、成本、候选执行者、证据依赖、验收谓词和回滚点。
5. 边支持 `exec / data / evidence / mutex` 四类，其中 mutex 不参与拓扑排序。
6. 环检测、唯一终点、拓扑排序、可执行节点查询、关键路径、1000 节点级图查询测试。
7. `update_node()`：节点定义变化后只失效其影响闭包。
8. `update_specification()`：GoalSpec 中途变化后增量重编译，保留未受影响节点的版本、结果和指纹。
9. `invalidate_node()`：证据/节点失效后只重算受影响子图。
10. 滚动窗口 `rolling_window_task_ids`，默认未来 10 个节点；完整图仍保留用于重放与影响分析。
11. acceptance condition 编译为 `acceptance_predicates`，并通过 Coordinator metadata 传给后续 Verifier。
12. 资源/隐私信息转换为 `placement`，兼容现有端-边-云调度桥。

## TaskNode 关键字段

旧 Coordinator 字段保持：

- `task_id`
- `description`
- `required_skill`
- `depends_on`
- `priority`

新增增强字段：

- `node_type`, `semantic_key`
- `required_skills`
- `inputs`, `outputs`
- `dependency_types`, `evidence_dependencies`, `mutex_with`
- `resource_requirements`
- `risk`, `estimated_cost`, `candidate_executors`
- `acceptance_predicates`
- `source_refs`, `rollback_checkpoint`
- `status`, `version`, `result`, `evidence`, `fingerprint`, `recompute_reason`

## 构建

```powershell
python -m todag build examples\long_task.json -o dag.json --coordinator-output coordinator-tasks.json
```

增强 GoalSpec 示例：

```powershell
python -m todag build examples\long_task_enriched.json -o dag-enriched.json
```

## Web API

```powershell
python -m todag serve --input examples\long_task_enriched.json --port 8780
```

接口：

- `POST /api/build`：首次构建。
- `GET /api/dag`：完整 TaskGraph 快照。
- `GET /api/ready`：当前可执行节点、滚动窗口和关键路径。
- `GET /api/coordinator-plan`：Coordinator 兼容计划。
- `PATCH /api/nodes/{task_id}`：原子修改单节点定义并局部失效。
- `PUT /api/specification`：更新六字段 GoalSpec 并局部重规划。
- `POST /api/nodes/{task_id}/result`：测试/演示环境写入节点结果。
- `POST /api/nodes/{task_id}/invalidate`：模拟证据或节点失效。

若 `acceptance_conditions` 为空，状态为 `needs_clarification`，禁止导出到 Coordinator，避免无验收标准执行。

## Coordinator 集成

```powershell
python -m dynamic_registry_sim.submit_todag --transport local examples\long_task_enriched.json
```

`coordinator_plan()` 继续输出旧字段，同时将验收谓词、风险、证据依赖、边类型、资源要求、回滚点等放入 `metadata`；旧消费者可以直接忽略新增元数据。

## 测试

```powershell
python -m unittest -v todag.tests.test_todag
python test_dag.py
python -m unittest -v test_scheduler.py
python -m unittest discover -s dynamic_registry_sim/tests -v
```

测试覆盖：六字段冻结、富 GoalSpec、局部重规划、证据依赖、端边云 placement、循环原子回滚、无验收阻断、1000 节点图查询以及 Coordinator 集成。
