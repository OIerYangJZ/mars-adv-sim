# 接收者条件低熵通信 V3

## 1. 目标

本版本不再只是“手工只发变化字段”，而是在 Coordinator 端维护接收者知识状态，对候选信息进行：

```text
Candidate TaskMessage
        ↓
Receiver KnowledgeDigest
        ↓
Receiver-conditioned state difference
        ↓
DecisionImpactEstimator
        ↓
priority / TTL / semantic dedup
     ↙        ↓        ↘
 duplicate   merge     send
   drop      defer
        ↓
ACK-backed knowledge update
        ↓
Decision contribution feedback
```

安全、权限、硬约束和关键证据只允许无损差分，不按低影响规则丢弃。

## 2. TaskMessage 顶层字段保持不变

任务消息仍严格只有十个顶层字段：

```text
message_id
sender
receiver
task_id
summary
facts
constraints
evidence_refs
priority
ttl
```

`TaskMessage.from_dict()` 现在会拒绝缺字段和额外顶层字段。

本次新增的 KnowledgeDigest、语义哈希、因果链、影响分数和 ACK 状态均为 Coordinator 内部状态或独立控制消息，**不会扩大 TaskMessage 顶层 JSON Schema**。

## 3. Receiver KnowledgeDigest

`ReceiverKnowledgeStore` 为 `(receiver, task_id)` 维护：

- `summary_hash`
- `fact_hashes`
- `constraint_hashes`
- `evidence_hashes`

KnowledgeDigest 只在接收者发送 `TASK_CONTEXT_ACK` 后推进。若 ACK 丢失，Coordinator 会保守地认为接收者可能不知道该事实，最多造成重发，不会造成关键事实被错误省略。

Agent 重注册时 Coordinator 清空该 Agent 的 Digest，从而兼容进程重启后本地缓存丢失。

## 4. 自动 Receiver Delta

Agent 仍可以按原接口提交 `TaskMessage`；Coordinator 合并到权威 `TaskContextStore` 后，不再机械转发原消息，而是：

1. 查询目标 Agent 的 KnowledgeDigest；
2. 比较权威状态中本次涉及的事实/约束/证据；
3. 已确认且内容哈希相同的字段不再发送；
4. `REMOVE` 仅在接收者确实已知该对象时发送；
5. 对每个接收者分别生成最小 TaskMessage。

因此同一条原始更新对不同 Agent 可以得到不同的实际消息。

## 5. 决策影响与 Message Policy

`DecisionImpactEstimator` 是可解释的在线基线：

- 硬约束、安全/隐私/权限关键词：高影响，立即发送；
- evidence 更新：高影响，立即发送；
- 删除操作：高影响，立即发送；
- 高 priority：缩短等待时间或立即发送；
- 普通低影响增量：进入短 merge window，等待与同任务后续更新合并。

当前策略对“仍然未知的唯一信息”不直接丢弃，只执行短暂延迟和合并；真正的 DROP 仅发生于：

- Receiver Digest 判定为已知；
- 相同 semantic content 正在飞行中；
- deferred message 已超过 TTL。

这是为了优先保证关键事实保真率。

## 6. Priority 与 TTL 已真正生效

- `priority` 参与即时发送/延迟窗口以及 deferred queue 排序；
- `ttl` 参与 deferred message 失效判断；
- inflight semantic suppression 也会在 TTL 后自动释放，避免 ACK 丢失导致永久抑制。

## 7. 语义去重

除原有 `message_id` 幂等外，新增 `semantic_fingerprint(TaskMessage)`：

```text
sender + receiver + task_id + summary + facts + constraints + evidence + priority + ttl
                            ↓
                      SHA-256 digest
```

它忽略随机 `message_id`，因此两个不同 message_id 但内容完全相同的消息也能抑制。

ACK 后再由 KnowledgeDigest 完成长期语义去重。

## 8. Evidence 支持撤销

`EvidenceRef` 保持原有 `{id, artifact_id, note}` 兼容格式；普通 add/replace 不增加字节。

当证据失效/污染时可使用：

```json
{
  "id": "source-1",
  "artifact_id": null,
  "note": null,
  "op": "remove"
}
```

无需为了删除一条错误证据发送完整 Snapshot。

## 9. Snapshot 顺序保护

旧版 Delta 与 Snapshot 使用两个 MQTT Topic，理论上存在跨 Topic 乱序覆盖风险。

升级版 Snapshot：

- 仍使用完全相同的十字段 TaskMessage；
- `message_id` 使用 `snapshot:<revision>:<uuid>`；
- Coordinator 将 Snapshot 和普通 Delta 都发到 `agent/<id>/context`；
- Agent 检测 `snapshot:` 前缀后执行 replace。

这样 Coordinator→Agent 的上下文更新位于同一 QoS1 Topic 中，避免 Snapshot 与 Delta 跨 Topic 重排。旧 snapshot Topic 仍保留接收兼容。

## 10. 因果链与贡献反馈

为了不扩大 TaskMessage，本版本将 `causal_parent` 和 `semantic_hash` 放在 Coordinator 的 `CausalLedger` 中：

- 每个 `(task_id, receiver)` 形成消息因果链；
- 记录 predicted impact；
- Agent 可调用 `report_context_contribution(..., changed_decision=True|False)` 上报真实后继决策贡献；
- 统计 `feedback_positive / feedback_negative`，供后续训练轻量分类器或消融分析。

## 11. 指标

`Coordinator.low_entropy_metrics()` 返回：

- candidate/sent message 数
- candidate/sent bytes
- byte reduction ratio
- dependency-free estimated token reduction
- deferred / merged / duplicate drop / expired drop
- ACK 数
- decision contribution feedback
- queue wait P95

注意：`token_reduction_est_ratio` 是统一估算口径，**不等同于具体 LLM tokenizer 的精确 Token 数**。正式论文/答辩应再使用目标模型 tokenizer 做离线精确统计。

`critical_fact_fidelity(expected, reconstructed)` 可用于关键事实无损回归测试。

## 12. 当前边界

已经实现：

- Receiver KnowledgeDigest
- 自动 receiver-conditioned delta
- priority 生效
- TTL 生效
- semantic hash 去重
- deferred merge
- evidence remove
- snapshot 顺序保护
- ACK-backed knowledge
- 中央 causal ledger
- decision contribution feedback hook
- bytes / estimated-token / queue telemetry

尚未强行加入：

- 学习型 DecisionImpactClassifier（当前为可解释规则基线）；
- 大模型 tokenizer 在线依赖（避免主链引入额外延迟）；
- 对象存储服务本身（当前继续使用 `artifact_id` 指针接口）。

这三项可以后续独立替换，不改变现有低熵通信 API。
