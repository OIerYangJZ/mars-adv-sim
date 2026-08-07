# ToDAG 升级说明

## 不变项

GoalSpec 顶层严格固定为六个字段：

- main_goal
- hard_constraints
- soft_preferences
- acceptance_conditions
- budget
- prohibitions

任何第七个顶层字段都会被拒绝。旧版纯字符串内容继续支持；增强信息只放在六个字段内部。

## 核心升级

- 富 GoalSpec 兼容：sub_goals、predicate、source_span、confidence、resource requirements 等。
- TaskNode 补齐输入/输出/风险/成本/证据依赖/资源需求/候选执行者/验收谓词/回滚点。
- 四类边：exec、data、evidence、mutex。
- 稳定语义 task_id，追加需求时不让无关任务整体换 ID。
- GoalSpec 变化支持局部重规划，保留无关分支结果和版本。
- 证据或节点失效支持影响闭包重算。
- stale 后继在前置重算完成后自动重新释放。
- 关键路径、ready task、滚动规划窗口。
- acceptance_conditions 缺失时进入 needs_clarification，禁止提交 Coordinator。
- Coordinator 兼容字段不变；新能力通过 metadata/placement 追加。
- Web API 新增 GoalSpec PUT 局部重规划、ready 查询和节点失效接口。
- 增加 1000 节点图查询与完整集成测试。

## 验证

- todag.tests.test_todag：12 tests passed
- test_scheduler.py：7 tests passed
- dynamic_registry_sim/tests：11 tests passed
- test_dag.py：DAG assertions passed
