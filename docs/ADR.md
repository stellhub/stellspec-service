# ADR: StellSpec 日志消费与 Elaticsearch 索引设计

## 1. Problem analysis

`stellspec-service` 是 StellSpec 日志平台的后端摄取服务，职责是从 Stellflow 消费日志消息，将日志归一化后写入 Elaticsearch，供后续检索、聚合、追踪关联和控制台查询使用。

当前代码链路如下：

```text
StellflowLogListener
  -> StellspecLogIngestionService
  -> LogPayloadNormalizer
  -> ElaticsearchLogWriter
  -> StellfluxElaticsearchClient.index(...)
```

当前实现中，服务不会主动调用 create index API 或 create index template API。`LogPayloadNormalizer` 根据日志事件时间生成索引名，`ElaticsearchLogWriter` 使用该索引名发起 `IndexRequest`。因此，实际索引创建依赖 Elaticsearch 的自动创建索引能力；如果集群禁用了自动创建索引，写入会失败。

当前索引名规则：

```text
${stellspec.logs.index-prefix}-${yyyy.MM.dd}
```

默认配置为：

```text
stellspec-logs-2026.05.25
```

当前文档 ID 规则：

```text
topic-partition-offset
```

当 Stellflow 消息没有有效 offset 时，退化为：

```text
topic-sha256(payload)
```

这保证同一条 Stellflow 消息被重复消费时写入同一个文档 ID，写入语义更接近幂等覆盖，而不是无限追加重复文档。

## 2. Design

### 2.1 一条日志一个文档

StellSpec 日志存储模型采用“一条逻辑日志事件对应一个 Elaticsearch 文档”。

这里的“一条日志”指一个完整的日志事件，而不是一行文本。对于普通业务日志，一条日志通常就是一条结构化 log record。对于异常日志，异常消息、异常类型和完整 stacktrace 应作为同一个日志事件的字段保存在同一个文档中，而不是把 stacktrace 的每一行拆成多个文档。

原因：

- 查询体验更符合用户认知，用户检索到的是一次异常事件，而不是几十行堆栈片段。
- traceId、spanId、service.name、severity、timestamp 等上下文只需要绑定一次，不会在多行 stacktrace 中重复膨胀。
- 聚合统计更准确，例如 ERROR 数量表示异常事件数量，而不是异常栈行数。
- 告警和去重更容易基于异常类型、异常消息、stacktrace hash、service.name 组合完成。

如果上游采集器已经把异常栈拆成多条日志消息，则 `stellspec-service` 不在消费端做跨消息重组。跨消息重组需要窗口、乱序处理和状态存储，复杂度较高，应优先在 OpenTelemetry Collector、应用日志采集器或日志 SDK 侧完成 multiline 合并。

### 2.2 文档写入方式

当前写入方式：

```text
IndexRequest(index = document.indexName, id = document.id, document = document.toSource())
```

文档字段当前包含：

| 字段 | 含义 |
| --- | --- |
| `@timestamp` | 日志事件时间，优先来自 `timeUnixNano`、`observedTimeUnixNano`、`timestamp`、`time`、`@timestamp` |
| `ingested_at` | 服务写入时间 |
| `severity_text` | 日志级别文本 |
| `severity_number` | OpenTelemetry severity number |
| `trace_id` | Trace ID |
| `span_id` | Span ID |
| `service_name` | 服务名，优先来自 resource attributes |
| `body` | 日志正文 |
| `message_key` | Stellflow 消息 key |
| `stellflow_topic` | Stellflow topic |
| `stellflow_partition` | Stellflow partition |
| `stellflow_offset` | Stellflow offset |
| `attributes` | 日志属性 |
| `resource` | OpenTelemetry resource attributes |
| `raw_payload` | 原始消息体，受 `stellspec.logs.include-raw-payload` 控制 |

当前实现会解析两类输入：

- 平铺 JSON，例如包含 `timestamp`、`severityText`、`serviceName`、`body`、`attributes` 的日志对象。
- OpenTelemetry logs JSON，例如 `resourceLogs[].scopeLogs[].logRecords[]`。

普通文本 payload 会被写入 `body`，同时在开启 `include-raw-payload` 时保留到 `raw_payload`。

### 2.3 异常栈存储方式

异常日志仍然是一条文档。推荐字段设计如下：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `exception.type` | `keyword` | 异常类名，例如 `java.lang.IllegalStateException` |
| `exception.message` | `text` + `keyword` 子字段 | 异常消息 |
| `exception.stacktrace` | `text`，不参与聚合 | 完整异常栈文本，保留换行 |
| `exception.stacktrace_hash` | `keyword` | 对规范化 stacktrace 计算 hash，用于去重和聚合 |
| `exception.escaped` | `boolean` | 异常是否逃逸到请求或任务边界 |

如果日志 payload 遵循 OpenTelemetry semantic conventions，可以优先从 attributes 中读取：

```text
exception.type
exception.message
exception.stacktrace
exception.escaped
```

如果 payload 是平铺 JSON，则也接受同名字段或嵌套对象：

```json
{
  "timestamp": "2026-05-25T01:02:03Z",
  "severityText": "ERROR",
  "serviceName": "order-service",
  "body": "create order failed",
  "exception": {
    "type": "java.lang.IllegalStateException",
    "message": "inventory unavailable",
    "stacktrace": "java.lang.IllegalStateException: inventory unavailable\n\tat ..."
  }
}
```

异常 stacktrace 不应拆分成数组行，除非控制台明确需要逐帧渲染。默认保留原始 stacktrace 文本，同时使用 `stacktrace_hash` 支持聚合。

### 2.4 索引设计

日志索引采用时间分区索引，默认按天切分：

```text
stellspec-logs-yyyy.MM.dd
```

原因：

- 日志数据天然按时间查询和保留。
- 按天索引便于生命周期管理和冷热分层。
- 单日索引可以降低单索引 shard 压力，也便于删除过期数据。

推荐使用 index template 管理 mapping，而不是长期依赖动态 mapping。索引模板匹配：

```text
stellspec-logs-*
```

推荐 mapping：

| 字段 | 推荐类型 | 说明 |
| --- | --- | --- |
| `@timestamp` | `date` | 主时间字段 |
| `ingested_at` | `date` | 写入时间 |
| `service_name` | `keyword` | 服务维度过滤和聚合 |
| `severity_text` | `keyword` | 日志级别过滤和聚合 |
| `severity_number` | `short` | OpenTelemetry 级别数值 |
| `trace_id` | `keyword` | trace 关联 |
| `span_id` | `keyword` | span 关联 |
| `body` | `text` + `keyword` 子字段 | 全文检索和精确匹配 |
| `message_key` | `keyword` | Stellflow 消息 key |
| `stellflow_topic` | `keyword` | 消息来源 topic |
| `stellflow_partition` | `integer` | 消息分区 |
| `stellflow_offset` | `long` | 消息 offset |
| `attributes` | `flattened` | 动态业务属性，避免 mapping 爆炸 |
| `resource` | `flattened` | OpenTelemetry resource attributes |
| `exception.type` | `keyword` | 异常类型 |
| `exception.message` | `text` + `keyword` 子字段 | 异常消息 |
| `exception.stacktrace` | `text` | 异常栈全文 |
| `exception.stacktrace_hash` | `keyword` | 异常去重 |
| `raw_payload` | `text`，可选 | 原始消息体 |

推荐 settings：

```json
{
  "index": {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "refresh_interval": "5s"
  }
}
```

生产环境可以按日志量调整 shard 数量。低吞吐环境保持 1 shard 更容易控制资源；高吞吐环境应根据单日数据量、节点数、查询并发和保留周期评估 shard 数量。

### 2.5 索引创建策略

短期策略：

- 保持当前 `IndexRequest` 写入方式。
- 本地和开发环境允许 Elaticsearch 自动创建 `stellspec-logs-*` 索引。
- 通过 `stellspec.logs.index-prefix` 和 `stellspec.logs.index-zone` 控制索引前缀和日期时区。

生产策略：

- 服务启动时检查 index template 是否存在。
- 如果不存在，则创建或提示部署侧预先创建。
- 禁止依赖动态 mapping 自动推断关键字段类型。
- 可进一步演进到 data stream 或 rollover alias，例如 `stellspec-logs-write`，但第一阶段保持日索引更直观。

推荐第一阶段仍使用普通时间索引，不直接引入 data stream。原因是当前服务需要先稳定日志 schema、查询条件和保留策略；等控制台查询模型稳定后，再决定是否迁移到 data stream + ILM。

### 2.6 查询模型

典型查询条件：

- 时间范围：`@timestamp`
- 服务：`service_name`
- 日志级别：`severity_text` 或 `severity_number`
- 链路关联：`trace_id`、`span_id`
- 关键词：`body`、`exception.message`、`exception.stacktrace`
- 来源定位：`stellflow_topic`、`stellflow_partition`、`stellflow_offset`
- 动态属性：`attributes.*`、`resource.*`

默认排序：

```text
@timestamp desc, ingested_at desc
```

如果出现同一事件时间下的大量日志，可用 `_id` 或 `stellflow_offset` 做稳定翻页补充排序。

## 3. Implementation

当前已经实现：

- 使用 `stellflux-spring-boot-starter-stellflow` 消费 Stellflow。
- 使用 `stellflux-spring-boot-starter-elaticsearch` 写入 Elaticsearch。
- 使用 `stellflux-spring-boot-starter-http` 暴露服务状态和手工写入接口。
- 使用 `LogPayloadNormalizer` 将 Stellflow 消息归一化为 `LogDocument`。
- 使用 `topic-partition-offset` 作为优先文档 ID，保证重复消费幂等覆盖。
- 使用事件时间按天生成索引名。

当前尚未实现但应作为后续任务补齐：

- 启动时创建或校验 `stellspec-logs-*` index template。
- 将异常字段从 `attributes` 中提升到顶层 `exception.*` 字段。
- 计算 `exception.stacktrace_hash`。
- 增加索引模板 JSON 和自动创建测试。
- 增加端到端测试，覆盖 Stellflow 消费一条日志后写入 Elaticsearch 的真实链路。

## 4. Complete code

当前核心代码入口：

- `src/main/java/io/github/stellhub/stellspec/service/log/StellflowLogListener.java`
- `src/main/java/io/github/stellhub/stellspec/service/log/StellspecLogIngestionService.java`
- `src/main/java/io/github/stellhub/stellspec/service/log/LogPayloadNormalizer.java`
- `src/main/java/io/github/stellhub/stellspec/service/log/ElaticsearchLogWriter.java`
- `src/main/java/io/github/stellhub/stellspec/service/log/domain/LogDocument.java`
- `src/main/resources/application.yaml`

当前服务写入行为可以概括为：

```text
一条 Stellflow 消息
  -> 一个 LogDocument
  -> 一个 Elaticsearch IndexRequest
  -> 一个 stellspec-logs-yyyy.MM.dd 文档
```

异常栈日志的目标设计可以概括为：

```text
一次异常事件
  -> 一个日志文档
  -> body 保存摘要
  -> exception.stacktrace 保存完整异常栈
  -> exception.stacktrace_hash 用于去重聚合
```
