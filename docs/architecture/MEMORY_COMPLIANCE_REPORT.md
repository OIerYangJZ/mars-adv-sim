# Memory 模块条件对齐报告

## 已落实

- 统一数据模型：MemoryRecord / ContextPack / MemoryType / VerificationStatus / ProcedureRecord / Snapshot。
- 配置：Redis、对象存储、TTL、召回上限、ContextPack 字符/Token/条目上限、快照周期均由环境变量读取。
- Repository：save/get/query/update/delete/query_by_node/query_by_nodes/query_by_evidence/get_many。
- Working Memory：`working:{run_id}:{node_id}`，保存 current_goal、active_constraints、recent_results、required_evidence、current_agent，并支持 TTL。
- Episodic Memory：`ingest_event(event)`，覆盖 BUILD_FAILED、TOOL_TIMEOUT、TASK_SUCCEEDED 等事件。
- Semantic Memory：兼容 canonical GoalSpec 的 `objective/hard_constraints/...` 和旧 `main_goal` 结构；硬约束和禁止项不可压缩。
- Procedural Memory：人工注册、成功/失败计数、达到成功阈值后可 VERIFIED。
- Ranking：TaskGraph、Evidence、importance、confidence、recency、semantic；权限和 STALE/REJECTED 为硬过滤。
- Retriever：按 Working → TaskGraph 邻域 → Evidence → 关键语义 → 向量候选 → 排序裁剪执行。
- ContextBuilder：固定 ContextPack；先裁历史，最后才裁普通事实；goal/hard_constraints/prohibitions 永不因预算竞争丢失。
- Invalidation：按 evidence_id 失效，并对 `depends_on_memory_ids` 做传递传播；REJECTED/STALE 不再召回。
- Snapshot：gzip + checksum + snapshot_id/key 恢复 + 损坏回退上一稳定快照 + Working Memory 一并恢复。
- VectorIndex：候选索引；支持 clear/rebuild，删除索引后 Repository 仍可恢复。
- Metrics：关键记忆召回率、ContextPack Token、全历史 Token、压缩率、召回时延、Snapshot 一致率、失效传播正确率。
- Service：唯一业务入口，封装工作记忆、事件、语义、程序、检索、ContextPack、失效、Snapshot、指标。
- Redis adapter：ConnectionPool、JSON、Hash/Set/ZSet、批量读写、TTL。
- Object store：本地文件首版，可替换 MinIO/S3；包含路径逃逸保护和 list_keys。
- mock：独立 GoalSpec / TaskGraph / Event / Evidence 输入。

## 验证

```text
PYTHONPATH=. pytest -q
12 passed
```

测试覆盖：

- 1000 步长程约束追问；
- 向量索引删除恢复；
- Snapshot 损坏回退及 apply 后一致性；
- 错误 Evidence 失效传播与污染阻断；
- TaskGraph/Evidence 显式召回；
- 硬约束权限隔离；
- ContextPack Token/字符预算与不可压缩约束；
- RedisRepository 索引更新、Evidence 查询和删除契约。

## 当前环境未执行项

交付容器没有安装 `redis` Python 包，也没有可用 Redis 服务，因此未在本环境执行真实 Redis 网络集成测试；RedisRepository 逻辑已通过 FakeRedisStore 契约测试。目标机应执行 `quick_check_redis.py` 或完整 Redis 集成测试。
