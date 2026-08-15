# 接收者条件低熵通信

## 1. 唯一通信策略入口

所有运行期上下文发送统一经过 `runtime.message_topology.MessageTopologyService`；MQTT 只是传输适配器，不再拥有另一套去重、压缩或拓扑策略。

```text
Task dependency + Assignment + Memory ContextPack
        ↓
MessageTopologyService
        ├─ dynamic Top-k topology / connectivity guard
        ├─ Receiver KnowledgeDigest
        ├─ receiver-conditioned delta
        ├─ semantic dedup
        ├─ priority / TTL
        └─ SEND / DEFER / MERGE / DROP
        ↓
MessageEnvelope
        ↓
transport (in-process / MQTT)
```

安全、权限、硬约束和关键 Evidence 只允许无损差分。

## 2. Wire contract

业务 `TaskMessage` 保持十个顶层字段：

```text
message_id, sender, receiver, task_id, summary,
facts, constraints, evidence_refs, priority, ttl
```

追踪字段、`content_hash`、`causal_parent` 和 token budget 通过 `TraceContext` / `MessageEnvelope` 在主链边界投影，不复制业务消息 Schema。

## 3. Receiver KnowledgeDigest

`KnowledgeDigestStore` 按 `(receiver, task_id)` 保存接收者已知的摘要、事实、约束与 Evidence 哈希。生成新消息时只发送“接收者尚未知且可能改变后继决策”的差分。

ACK/投递确认后才推进 Digest；若确认丢失，系统宁可重复发送，也不能错误省略硬约束或关键 Evidence。

## 4. 去重、优先级和 TTL

- semantic fingerprint 忽略随机 `message_id`，用于内容级重复抑制；
- `priority` 控制立即发送或短窗口合并；
- `ttl` 控制 deferred/inflight 消息失效；
- 低价值但仍未知的唯一信息优先 `DEFER/MERGE`，不随意丢弃；
- 真正 `DROP` 只用于已知、重复、过期或被动态拓扑裁剪的消息。

## 5. 动态拓扑

拓扑候选边综合任务依赖、Assignment、priority、Agent reliability 与 latency score。`TopologyManager` 使用 Top-k 稀疏化并通过 connectivity guard 保证必要连通；节点/任务变化只触发局部重构。

`MosaicMainChain._sync_topology()` 是 Execution TaskGraph 与通信拓扑之间的唯一运行期桥接点。

## 6. Evidence 与大内容

业务消息只携带 Evidence 指针。`MessageTopologyService.store_evidence()` 可通过统一 `ContentAddressedObjectStore` 保存大内容；主仓只保留一个 `storage.LocalObjectStore` 实现。

## 7. 指标边界

当前模块记录消息数、字节、queue wait、去重/合并/过期等遥测。估算 Token 只能用于工程趋势；正式验收必须用目标模型 tokenizer 统计真实 input/output Token，并与全连接自由文本基线比较。
