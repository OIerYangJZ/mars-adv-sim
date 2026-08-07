# V2 → Receiver-Conditioned V3 修改清单

1. 新增 `low_entropy.py`
   - `KnowledgeDigest`
   - `ReceiverKnowledgeStore`
   - `ReceiverConditionedCompressor`
   - `DecisionImpactEstimator`
   - `LowEntropyOutbox`
   - semantic content hash
   - `CausalLedger`
   - `LowEntropyMetrics`
   - critical fact fidelity

2. `task_messages.py`
   - TaskMessage 严格限制十个顶层字段；
   - EvidenceRef 增加兼容式 remove 能力，普通消息格式不变。

3. `task_context.py`
   - evidence remove；
   - snapshot message_id 带 authority revision；
   - snapshot 与 delta 可统一使用同一 context Topic。

4. `coordinator.py`
   - 接收者条件差分；
   - ACK-backed KnowledgeDigest；
   - impact gate；
   - priority/TTL/merge；
   - semantic in-flight dedup；
   - causal ledger；
   - contribution feedback；
   - metrics API。

5. `agent_runtime.py`
   - context ACK；
   - snapshot prefix 自动 replace；
   - contribution feedback API。

6. 测试
   - 原 10 个测试保持通过；
   - 新增 receiver digest、semantic dedup、TTL、priority/impact、merge、Evidence REMOVE、严格十字段、snapshot fencing、critical fidelity 测试。
