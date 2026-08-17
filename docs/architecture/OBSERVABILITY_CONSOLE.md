# MOSAIC-Ω V3 Core Observability Console

## 1. 设计目标

Console 不是第二个控制平面，也不是第二套 TaskStore。它只把核心平台已有事实源投影成负责人可读的运行界面：

```text
Authoritative Core
  ├─ EventStore
  ├─ Task projection
  ├─ Capability Registry / Scheduler
  ├─ MessageTopology telemetry
  ├─ MemoryService
  └─ Verifier / Recovery events
          │
          ▼
ObservabilityRuntime (read-only projection)
  ├─ JSONL structured log
  ├─ MetricRegistry
  └─ atomic dashboard snapshot
          │
          ▼
ConsoleDataSource (read-only filesystem)
          │
          ▼
HTTP API + self-contained Web UI
```

任何 Console 文件被删除，都不影响 EventStore、任务状态、Evidence、Memory 或调度结果；下一次 capture 可以重新生成。

## 2. 新增目录

```text
src/mosaic_omega/observability/
  logging.py       # EventStore -> structured JSONL
  metrics.py       # derived counters/gauges/histograms
  tracing.py       # trace grouping and duration projection
  projections.py   # one dashboard schema
  snapshots.py     # atomic projection persistence
  runtime.py       # single observability facade

src/mosaic_omega/console_api/
  source.py        # read-only snapshot reader

apps/console/
  main.py
  backend/server.py
  frontend/index.html
  frontend/assets/style.css
  frontend/assets/app.js
```

## 3. 单一事实源原则

### Console 可以做

- 读 EventStore 投影结果。
- 读 Capability / Assignment。
- 读 TopologySnapshot 和 telemetry。
- 读 MemoryService 的 observability projection。
- 读 Verification / Recovery Event。
- 过滤、排序、可视化、导出负责人所需信息。

### Console 永远不能做

- 修改 Task state。
- 创建 Assignment。
- 直接操作 PostgreSQL / Redis。
- 调用 ToolRuntime。
- 触发 Recovery。
- 把页面缓存当作运行事实源。

HTTP Server 对 POST / PUT / PATCH / DELETE 统一返回 `405 read_only_console`。

## 4. Dashboard 页面

1. **Overview**：Run 状态、成功率、E2E latency、Event、Evidence、Message、Agent。
2. **TaskGraph**：DAG、状态、risk、priority、Assignment、acceptance、Evidence 数量。
3. **Topology**：动态稀疏节点/边、λ2/connectivity、边 score、Topology telemetry。
4. **Communication**：SEND/MERGE/DEFER/DROP 决策流、Queue P95、MQTT RTT、真实 Token 状态。
5. **Scheduler**：Assignment、policy、cost breakdown、异构 CapabilityProfile。
6. **Memory**：Working/Episodic/Semantic/Procedural、Recall、Snapshot、ContextPack 压缩。
7. **Recovery**：RECOVERY/REPLAN/ROLLBACK/SAFE_STOP/ERROR 的 Event-sourced timeline。
8. **Evidence**：Evidence Manifest、SHA-256、VerificationResult。
9. **Event Trace**：run/task/actor/trace 的统一 EventStore 时间线。

## 5. Token 数据原则

若运行没有真实 tokenizer/model usage，Console 显示：

```text
INSUFFICIENT DATA
```

不会使用 `字符数 / 4` 等估算值伪装成真实 Token。只有 `MessageTopologyTelemetry.model_input_tokens` / `model_output_tokens` 有测量样本时才显示 `MEASURED`。

## 6. 运行方式

### 一键 Demo + Console

```bash
PYTHONPATH=src:. ./scripts/demo_console.sh
```

浏览器打开：

```text
http://127.0.0.1:8080
```

### 只查看已有运行

```bash
PYTHONPATH=src:. python apps/console/main.py \
  --snapshot-dir .mosaic_workspace/mainchain-demo/observability
```

### Docker Compose

核心执行进程与 Console 共用 `mosaic_workspace` volume，但 Console 以 `:ro` 方式挂载：

```bash
docker compose -f deploy/docker-compose.yml --profile console up console
```

Console 默认端口 `8080`。

## 7. Runtime 输出

每个 Workspace：

```text
.mosaic_workspace/.../observability/
  latest.json
  runs/<run_id>.json
  logs/events.jsonl
```

`events.jsonl` 至少包含：

- timestamp
- level
- service
- trace_id
- run_id
- task_id / node_id
- actor_id
- model_id
- event_type
- latency_ms
- error_code
- schema_version

## 8. 冗余防护

`tests/unit/test_architecture_guard.py` 检查 Console 不导入 EventStore、Scheduler、ToolRuntime、RecoveryEngine、VerifierService 等写侧 authority。

`tests/unit/observability/test_observability_console.py` 验证：

- MainChain 自动生成 dashboard snapshot。
- JSONL 日志字段齐全。
- ConsoleDataSource 只读。
- 前端无 CDN / 外部 JS 依赖。

这样 ROS Repair、Data Research 和未来机器人场景都共享同一 Console，不允许每个场景单独开发 Dashboard。
