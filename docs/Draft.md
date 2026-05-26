下面是一篇可直接用于技术博客/公众号的论文式文章草稿。文中采用 **Elasticsearch 官方文档、Elastic Common Schema 官方规范** 作为主要依据。

---

# 超大型企业基于 Elasticsearch 存储应用日志的工程实践研究

## 摘要

在超大型企业场景中，应用日志具有持续写入、时间序列、字段结构复杂、租户隔离要求高、异常日志突发、重复日志密集、存储成本敏感等特征。Elasticsearch 官方文档将 data stream 定义为面向 append-only 时间序列数据的抽象层，适用于日志、事件、指标等持续生成的数据；Index Lifecycle Management 可对日志类时间序列索引执行 rollover、保留、删除等自动化管理。本文基于 Elasticsearch 与 Elastic Common Schema 官方文档，系统论述超大型企业使用 Elasticsearch 存储应用日志时的索引创建、字段类型、文档结构、异常栈存储、重复日志合并、多租户隔离、大流量应用治理与超长日志处理方法。([Elastic][1])

**关键词**：Elasticsearch；应用日志；Data Stream；ILM；ECS；多租户；异常日志；日志合并；超长日志

---

## 1. 引言

应用日志属于典型的时间序列事件数据。Elastic 官方文档指出，data stream 适合存储日志、事件、指标等持续生成的数据，并通过多个 backing index 承载数据，同时向使用者暴露一个统一的命名资源。data stream 要求每个文档包含 `@timestamp` 字段，该字段需要映射为 `date` 或 `date_nanos` 类型；如果 index template 未显式声明，Elasticsearch 会将 `@timestamp` 按默认 `date` 字段处理。([Elastic][1])

因此，超大型企业日志平台不应以“每天手工创建普通 index”为默认模型，而应以 **data stream + index template + lifecycle policy + 明确 mapping** 作为基础模型。该模型的核心目标包括：控制字段膨胀、控制 shard 数量、控制冷热数据成本、保障查询字段可聚合、保障异常栈可检索、支持租户隔离与大流量应用分流。

---

## 2. 索引创建模型

### 2.1 应优先使用 Data Stream 而不是裸 Index

应用日志一般具备以下特征：包含时间戳、主要执行写入操作、很少对历史文档执行更新或删除。Elasticsearch 官方文档给出的 data stream 适用条件包括：数据包含 timestamp 字段、主要执行 indexing 请求、偶尔更新和删除、通常不显式指定 `_id`，或指定 `_id` 时接受 first-write-wins 语义。([Elastic][1])

因此，应用日志的基础命名模型应采用：

```text
logs-<dataset>-<namespace>
```

例如：

```text
logs-order-service-prod
logs-payment-service-prod
logs-gateway-access-prod
logs-bigapp-access-prod
```

其中：

| 部分            | 含义                   | 示例                               |
| ------------- | -------------------- | -------------------------------- |
| `logs`        | 数据类型                 | 日志                               |
| `<dataset>`   | 日志数据集，一般对应应用、模块或日志类型 | `order-service`、`gateway-access` |
| `<namespace>` | 环境、租户、区域或隔离域         | `prod`、`tenant-a`、`sg-prod`      |

Elastic APM 文档中也说明，data stream 对日志、指标、trace 等 append-only 时间序列数据适用，并带来减少单个 index 字段数量、更细粒度数据控制、灵活命名、更少 ingest 权限等收益。([Elastic][2])

### 2.2 Index Template 应声明 Settings、Mappings 与 Lifecycle

data stream 依赖匹配的 index template。官方文档明确说明，data stream 需要一个匹配的 index template，该 template 包含 backing index 的 mappings、settings，并定义 data stream 使用的 ILM policy。([Elastic][1])

日志 index template 至少应包含以下内容：

```json
{
  "index_patterns": ["logs-*-*"],
  "data_stream": {},
  "template": {
    "settings": {
      "index.lifecycle.name": "logs-hot-warm-cold-delete",
      "index.mapping.total_fields.limit": 1000,
      "index.mapping.ignore_above": 8191,
      "index.refresh_interval": "10s",
      "number_of_shards": 3,
      "number_of_replicas": 1
    },
    "mappings": {
      "dynamic": false,
      "properties": {
        "@timestamp": { "type": "date" },
        "message": { "type": "match_only_text" },
        "log.level": { "type": "keyword" },
        "log.logger": { "type": "keyword" },
        "service.name": { "type": "keyword" },
        "service.version": { "type": "keyword" },
        "service.environment": { "type": "keyword" },
        "host.name": { "type": "keyword" },
        "trace.id": { "type": "keyword" },
        "span.id": { "type": "keyword" },
        "event.dataset": { "type": "keyword" },
        "event.hash": { "type": "keyword" },
        "error.type": { "type": "keyword" },
        "error.message": { "type": "match_only_text" },
        "error.stack_trace": { "type": "wildcard" },
        "labels": { "type": "flattened" },
        "attributes": { "type": "flattened" }
      }
    }
  }
}
```

该配置包含三个关键约束。

第一，`dynamic: false` 用于阻止日志中的未知字段自动扩展 mapping。Elasticsearch 默认允许动态 mapping，新字段写入时会自动加入 mapping；官方文档将失控字段增长称为 mapping explosion，并指出过多字段会导致搜索变慢、JVM 内存压力升高和启动时间延长。([Elastic][3])

第二，`index.mapping.total_fields.limit` 用于限制字段数量。官方文档说明该设置用于限制 index 中字段 mapping 数量，默认值为 `1000`，提高该值可能导致性能下降和内存问题。([Elastic][4])

第三，`labels`、`attributes` 等不确定 key 集合应使用 `flattened`。官方文档说明，当对象字段包含大量或未知唯一 key 时，`flattened` 可将整个对象映射为单个字段，从而避免大量不同 field mapping 引发 mapping explosion。([Elastic][5])

---

## 3. Lifecycle 与 Shard 策略

### 3.1 生命周期管理

日志类数据应通过 ILM 或 data stream lifecycle 管理。Elasticsearch 官方文档说明，ILM 可自动管理日志和指标等时间序列索引，并可执行 rollover、归档历史索引、删除过期索引等操作。([Elastic][6])

一个典型策略为：

```json
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_primary_shard_size": "50gb",
            "max_age": "1d"
          }
        }
      },
      "warm": {
        "min_age": "3d",
        "actions": {
          "forcemerge": {
            "max_num_segments": 1
          }
        }
      },
      "cold": {
        "min_age": "14d",
        "actions": {}
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```

这类策略的目标不是固定每日一个 index，而是通过 rollover 让 backing index 在达到大小或时间条件时切换。data stream 官方文档也建议使用 ILM 在 write index 达到指定 age 或 size 时自动 rollover。([Elastic][1])

### 3.2 Shard 数量控制

超大型企业日志系统应避免 shard 过多或 shard 过大。Elastic 官方文档说明，每个 index、shard、segment 和 field 都有开销，时间序列数据适合使用 data stream 与 ILM；同时，删除整个 index 比逐条删除文档更能立即释放文件系统资源。([Elastic][7])

因此，日志保留策略应优先通过生命周期删除 backing index，而不是通过 `delete_by_query` 对历史日志做逐条删除。逐条删除会产生 deleted document，直到 segment merge 后才释放资源；删除整个 index 则可直接释放文件系统资源。([Elastic][7])

---

## 4. 字段类型设计

### 4.1 应设置为 Keyword 的字段

Elastic 官方文档说明，`keyword` 用于结构化内容，例如 ID、email、hostname、status code、zip code、tag；keyword 字段常用于 sorting、aggregations 与 term-level queries，不应用于全文搜索。([Elastic][8])

应用日志中以下字段应设置为 `keyword`：

| 字段                          | 类型                  | 原因                |
| --------------------------- | ------------------- | ----------------- |
| `service.name`              | `keyword`           | 服务维度过滤、聚合         |
| `service.version`           | `keyword`           | 版本维度过滤            |
| `service.environment`       | `keyword`           | 环境维度过滤            |
| `tenant.id`                 | `keyword`           | 多租户隔离与过滤          |
| `app.id`                    | `keyword`           | 应用维度聚合            |
| `log.level`                 | `keyword`           | 日志级别过滤            |
| `log.logger`                | `keyword`           | logger/class 维度过滤 |
| `host.name`                 | `keyword`           | 主机维度过滤            |
| `host.ip`                   | `ip` 或 `keyword`    | IP 查询或精确过滤        |
| `trace.id`                  | `keyword`           | trace 精确检索        |
| `span.id`                   | `keyword`           | span 精确检索         |
| `transaction.id`            | `keyword`           | 请求链路精确检索          |
| `event.dataset`             | `keyword`           | 数据集隔离             |
| `event.hash`                | `keyword`           | 重复日志识别            |
| `error.type`                | `keyword`           | 异常类型聚合            |
| `error.code`                | `keyword`           | 错误码聚合             |
| `http.request.method`       | `keyword`           | HTTP 方法过滤         |
| `http.response.status_code` | `short` 或 `integer` | 状态码聚合、范围统计        |
| `url.path`                  | `keyword`           | API 路径聚合          |
| `kubernetes.namespace`      | `keyword`           | K8s 命名空间过滤        |
| `kubernetes.pod.name`       | `keyword`           | Pod 维度过滤          |

`text` 与 `keyword` 的语义不同。Elasticsearch 官方文档说明，`text` 字段会被分析，用于全文搜索；`keyword` 字符串保持原样，用于过滤和排序。([Elastic][9])

### 4.2 Message 字段

`message` 是日志展示与全文检索的主要字段，不应设置为单纯 `keyword`。对于日志正文，推荐使用：

```json
"message": {
  "type": "match_only_text"
}
```

ECS 对 `error.message` 使用 `match_only_text`，用于存储错误消息。([Elastic][10])

对于需要同时全文搜索和精确聚合的短字段，可以使用 multi-fields，例如：

```json
"request.path": {
  "type": "text",
  "fields": {
    "keyword": {
      "type": "keyword",
      "ignore_above": 1024
    }
  }
}
```

Elasticsearch 官方文档说明，multi-fields 可将同一个字符串字段按不同方式索引，例如同时作为 `text` 用于全文检索、作为 `keyword` 用于排序或聚合。([Elastic][9])

### 4.3 不确定标签与业务扩展字段

业务日志经常包含动态字段，例如：

```json
{
  "attributes": {
    "userId": "10001",
    "orderId": "A123",
    "experimentGroup": "B",
    "customKeyFromBusiness": "value"
  }
}
```

这些字段不应无限制展开为 mapping 字段。Elastic 官方文档指出，`flattened` 可将整个对象作为单个字段映射，适合大量或未知唯一 key 的对象。([Elastic][5])

因此，业务自定义 KV 推荐统一放入：

```json
"labels": { "type": "flattened" },
"attributes": { "type": "flattened" }
```

而不是将每个业务 key 提升为顶层字段。

---

## 5. 日志文档结构

### 5.1 文档应遵循 ECS 语义

Elastic Common Schema 是 Elastic 官方定义的通用字段规范，用于在 Elasticsearch 中存储日志和指标等事件数据；ECS 指定字段名、Elasticsearch 数据类型、字段描述与示例用法。([Elastic][11])

一个应用日志文档可采用如下结构：

```json
{
  "@timestamp": "2026-05-25T01:23:45.678Z",
  "message": "Create order failed, orderId=10001",
  "log": {
    "level": "ERROR",
    "logger": "com.stellhub.order.OrderService",
    "origin": {
      "file": {
        "name": "OrderService.java",
        "line": 128
      },
      "function": "createOrder"
    }
  },
  "service": {
    "name": "order-service",
    "version": "1.3.7",
    "environment": "prod"
  },
  "event": {
    "dataset": "order-service.error",
    "kind": "event",
    "category": ["application"],
    "type": ["error"],
    "hash": "f35a9b2e..."
  },
  "trace": {
    "id": "4bf92f3577b34da6a3ce929d0e0e4736"
  },
  "span": {
    "id": "00f067aa0ba902b7"
  },
  "tenant": {
    "id": "tenant-a"
  },
  "error": {
    "type": "java.lang.NullPointerException",
    "message": "Cannot invoke \"User.getId()\" because user is null",
    "stack_trace": "java.lang.NullPointerException: Cannot invoke ..."
  },
  "labels": {
    "region": "sg",
    "az": "sg-a",
    "cluster": "prod-01"
  },
  "attributes": {
    "orderId": "10001",
    "bizCode": "CREATE_ORDER"
  }
}
```

ECS 中 `event` 字段用于描述日志或指标事件的上下文；日志事件必须包含事件发生时间。([Elastic][12]) ECS 中 `log.*` 字段用于描述日志机制或日志传输信息，例如 `log.level`、`log.logger`、`log.origin.file.line` 等。([Elastic][13])

### 5.2 文档写入应使用 Bulk API

高吞吐日志写入不应逐条请求 Elasticsearch。官方 Bulk API 文档说明，Bulk API 可在单个请求中执行多个 `index`、`create`、`delete`、`update` 动作，从而降低开销并显著提升 indexing speed。([Elastic][14])

Elastic 官方性能文档还说明，bulk request 通常比 single-document index request 具有更好的性能；最佳 bulk 大小需要基准测试，从 100、200、400 逐步增加，直到写入速度进入平台期。([Elastic][15])

---

## 6. 异常栈存储方法

异常日志应拆分为结构化字段与原始异常栈字段。ECS 官方定义中，`error.code` 是错误码，类型为 `keyword`；`error.id` 是错误唯一标识，类型为 `keyword`；`error.message` 是错误消息，类型为 `match_only_text`；`error.stack_trace` 是 plain text 形式的异常栈，类型为 `wildcard`。([Elastic][10])

因此，异常日志不应只写入一段大字符串：

```json
{
  "message": "Exception in thread ..."
}
```

更合理的结构为：

```json
{
  "message": "Create order failed",
  "error": {
    "type": "java.lang.NullPointerException",
    "message": "Cannot invoke \"User.getId()\" because user is null",
    "stack_trace": "java.lang.NullPointerException: Cannot invoke ...\n\tat ..."
  }
}
```

其中：

| 字段                     | 用途       |
| ---------------------- | -------- |
| `error.type`           | 异常类名聚合   |
| `error.message`        | 异常消息全文搜索 |
| `error.stack_trace`    | 栈内容检索    |
| `event.hash`           | 相同异常归并   |
| `log.origin.file.name` | 源文件定位    |
| `log.origin.file.line` | 行号定位     |
| `trace.id`             | 链路追踪关联   |

异常栈字段不应作为普通 `keyword` 存储。`keyword` 面向结构化精确值，而 `wildcard` 官方定义为适合大型值或高基数字段的非结构化机器生成内容。([Elastic][8])

---

## 7. 大量重复日志的合并

### 7.1 重复日志识别

大量重复日志应通过稳定 fingerprint 识别。Elasticsearch ingest fingerprint processor 可根据文档字段计算 hash，并将结果写入目标字段；官方文档说明 fingerprint processor 用于计算文档内容 hash，可用于 content fingerprinting。([Elastic][16])

日志平台可基于以下字段生成 `event.hash`：

```text
tenant.id
service.name
service.environment
log.level
log.logger
error.type
normalized.message
normalized.stack_trace.top_frame
```

不宜直接使用完整 `message` 或完整 `stack_trace` 作为 fingerprint 输入，因为其中可能包含 orderId、userId、traceId、timestamp 等高变字段。更合理的方式是先做归一化：

```text
Create order failed, orderId=10001
Create order failed, orderId=10002
```

归一化为：

```text
Create order failed, orderId=<num>
```

再计算 fingerprint。

### 7.2 合并文档模型

重复日志合并后可形成两类文档。

第一类是原始样本日志：

```json
{
  "@timestamp": "2026-05-25T01:00:01.000Z",
  "event.kind": "event",
  "event.hash": "abc123",
  "message": "Create order failed, orderId=10001",
  "error.stack_trace": "...",
  "sampled": true
}
```

第二类是聚合日志：

```json
{
  "@timestamp": "2026-05-25T01:00:00.000Z",
  "event.kind": "metric",
  "event.hash": "abc123",
  "service.name": "order-service",
  "log.level": "ERROR",
  "error.type": "java.lang.NullPointerException",
  "log.pattern": "Create order failed, orderId=<num>",
  "first_seen": "2026-05-25T01:00:00.000Z",
  "last_seen": "2026-05-25T01:00:59.999Z",
  "occurrence_count": 184920,
  "sample_message": "Create order failed, orderId=10001",
  "sample_stack_trace": "java.lang.NullPointerException..."
}
```

该模型将“可诊断样本”和“统计计数”分离，避免在异常风暴期间写入大量完全相同的文档。

### 7.3 是否应对同一时间的大量相同日志做过滤与合并

在同一时间窗口内的大量相同日志应在 Elasticsearch 写入前完成合并，而不是写入后再依赖查询聚合修正。原因有三点。

第一，Elasticsearch data stream 是面向 append-only 的时间序列写入模型；官方文档说明，data stream 不能直接对已有文档执行 update 或 delete 请求，若需要更新或删除，需要直接访问 backing index，或使用 update by query / delete by query。([Elastic][1])

第二，若业务希望“相同 `_id` 的后写覆盖前写”，官方文档明确指出，频繁发送相同 `_id` 并期望 last-write-wins 时，应考虑使用 index alias + write index，而不是 data stream。([Elastic][17])

第三，重复日志在进入 Elasticsearch 前合并，可以直接减少 bulk 写入量、segment 增长、磁盘占用、merge 压力与查询负担。Elasticsearch 官方文档已说明 bulk 写入性能与 shard/indexing 策略相关，写入策略与 shard 布局会显著影响 indexing speed。([Elastic][15])

因此，日志采集链路中应在 Kafka Consumer、Flink、Logstash Aggregate、OpenTelemetry Collector 自定义 processor 或专用日志聚合服务中完成窗口合并，再写入 Elasticsearch。

---

## 8. 大量相同异常的过滤与合并

大量相同异常不应无条件逐条写入完整异常栈。更适合的处理模型是：

```text
异常归一化 → fingerprint → 时间窗口聚合 → 样本保留 → 计数写入 → 告警触发
```

建议保留以下信息：

| 信息                     | 处理方式              |
| ---------------------- | ----------------- |
| 第一条异常                  | 完整保留              |
| 最近一条异常                 | 完整保留              |
| 每个窗口计数                 | 聚合写入              |
| 示例 trace.id            | 保留若干样本            |
| 完整 stack trace         | 样本保留，不对每条重复异常重复写入 |
| occurrence_count       | 聚合字段              |
| first_seen / last_seen | 聚合字段              |

这不是简单“丢弃异常”，而是把重复异常转换为 **样本 + 计数 + 时间窗口** 的数据结构。对故障定位而言，完整重复写入 100 万条相同异常栈并不会增加 100 万倍诊断信息；它主要增加存储、写入与查询成本。该判断来自数据模型本身：相同 fingerprint 的异常在同一窗口内具有相同错误类型、相同归一化 message、相同关键栈帧，诊断信息应由样本提供，规模信息应由计数字段提供。

---

## 9. 多租户场景处理

### 9.1 租户字段

多租户日志文档必须包含租户字段：

```json
{
  "tenant": {
    "id": "tenant-a",
    "name": "Tenant A"
  }
}
```

`tenant.id` 应设置为 `keyword`，用于过滤、聚合与访问控制。`keyword` 官方适用于 ID、hostname、status code、tag 等结构化内容，并常用于 term-level query、aggregation、sorting。([Elastic][8])

### 9.2 租户隔离模型

多租户可分为三种模式：

| 模式                              | 适用场景             | 示例                  |
| ------------------------------- | ---------------- | ------------------- |
| 共享 data stream + `tenant.id` 过滤 | 租户多、单租户日志量小、成本敏感 | `logs-app-prod`     |
| 按大租户拆 data stream               | 头部租户日志量大、隔离要求高   | `logs-app-tenant-a` |
| 独立集群                            | 合规、安全、资源隔离强要求    | 专属 ES cluster       |

在超大型企业中，不应对每个小租户创建大量独立 index。原因是每个 index、shard、segment、field 都存在开销；官方 shard sizing 文档明确指出这些对象都有开销。([Elastic][7])

因此，默认模型应为“共享 data stream + tenant.id 字段 + 权限过滤 + 大租户拆分”。对头部租户、大客户、强合规租户再提升隔离等级。

---

## 10. 大流量应用的特殊处理

对于网关、推荐、广告、搜索、支付核心链路等大流量应用，不应与普通应用日志共用同一个 data stream。应独立拆分：

```text
logs-gateway-access-prod
logs-gateway-error-prod
logs-search-access-prod
logs-search-slowlog-prod
logs-payment-error-prod
```

拆分原则包括：

| 拆分维度                      | 原因               |
| ------------------------- | ---------------- |
| access log 与 error log 分离 | 查询模式、保留周期、字段结构不同 |
| 大流量应用独立 data stream       | 防止写入热点影响普通应用     |
| 慢日志独立 data stream         | 查询周期长、诊断价值高      |
| 审计日志独立 data stream        | 保留周期和合规要求不同      |
| debug 日志独立 data stream    | 生命周期短、写入量大       |

Elasticsearch data tier 官方文档说明，数据层用于在性能、成本和可访问性之间平衡，不同 tier 具有不同硬件与存储特征；同一 tier 内节点应具有相同硬件配置，以避免 hot spotting。([Elastic][18])

因此，大流量应用的日志应配置独立 lifecycle、独立 rollover 条件、独立 shard 数量与独立冷热策略。例如：

```text
普通应用 error log：保留 30 天
普通应用 info log：保留 7 天
网关 access log：热层保留 1 天，冷层保留 7 天
支付 error log：热层保留 7 天，总保留 90 天
审计日志：独立集群或独立 data stream，长期保留
```

---

## 11. 超长日志处理

### 11.1 超长字段不应作为 Keyword 完整索引

Elasticsearch `ignore_above` 官方文档说明，超过 `ignore_above` 设置的字符串不会被索引或存储；但如果 `_source` 启用，原始值仍会保留在 `_source` 中。该设置也可用于避免 Lucene term byte-length limit。([Elastic][19])

因此，超长日志处理应遵循：

```text
可检索摘要字段：进入 Elasticsearch index
完整原文字段：保留在 _source 或外部对象存储
精确聚合字段：设置 ignore_above
异常栈：存 error.stack_trace，不作为 keyword
```

### 11.2 推荐结构

```json
{
  "message": "Large response body detected",
  "message_truncated": true,
  "message_length": 983421,
  "message_preview": "first 4096 chars...",
  "message_hash": "sha256:...",
  "log": {
    "original": "完整日志，必要时仅保留在对象存储"
  },
  "external": {
    "storage": "s3",
    "object_key": "logs/2026/05/25/abc123.log"
  }
}
```

对超长日志应采用以下规则：

| 类型                       | 处理方式                 |
| ------------------------ | -------------------- |
| 超长 `message`             | 截断展示字段，保留 hash       |
| 超长异常栈                    | 保留前 N 行、根因栈帧、hash、样本 |
| 超长 request/response body | 默认不全文入 ES，进入对象存储     |
| 超长业务 attributes          | 放入 `flattened` 或外部存储 |
| 超长 keyword 值             | 设置 `ignore_above`    |
| 原文审计要求                   | 进入对象存储，ES 仅存索引元数据    |

---

## 12. 查询与聚合约束

日志平台常用 terms aggregation 对 `service.name`、`log.level`、`error.type`、`event.hash` 做 TopN 统计。Elasticsearch 官方文档说明，默认不能在 `text` 字段上执行 terms aggregation，应使用 `keyword` 子字段；启用 `fielddata` 会显著增加内存使用。([Elastic][20])

因此，所有需要 TopN、分组、过滤、排序的字段必须显式设置为 `keyword`、数值、`ip`、`boolean` 等可聚合类型，而不是 `text`。

深分页也需要限制。Elasticsearch 官方文档说明，不应使用 `from` 和 `size` 做过深分页，因为每个 shard 需要加载当前页和之前页结果，可能显著增加内存和 CPU 使用；默认无法用 `from` 和 `size` 翻过 10000 条结果，超过时应使用 `search_after`。([Elastic][21])

---

## 13. 标准落地方案

### 13.1 Index Template 标准

| 配置项    | 建议                                           |
| ------ | -------------------------------------------- |
| 存储模型   | data stream                                  |
| 命名     | `logs-<dataset>-<namespace>`                 |
| 时间字段   | `@timestamp: date`                           |
| 动态字段   | `dynamic: false`                             |
| 自定义 KV | `flattened`                                  |
| 字段上限   | 保持默认或谨慎调整 `index.mapping.total_fields.limit` |
| 超长字符串  | 设置 `index.mapping.ignore_above`              |
| 生命周期   | ILM 或 data stream lifecycle                  |
| 删除策略   | 删除 index/backing index，不逐条删除文档               |

### 13.2 字段标准

| 字段类别        | 类型                    |
| ----------- | --------------------- |
| ID、枚举、状态、名称 | `keyword`             |
| 正文日志        | `match_only_text`     |
| 异常消息        | `match_only_text`     |
| 异常栈         | `wildcard`            |
| 时间          | `date` 或 `date_nanos` |
| 数量、耗时、大小    | `long`、`double`       |
| HTTP 状态码    | `short` 或 `integer`   |
| IP          | `ip`                  |
| 动态业务标签      | `flattened`           |

### 13.3 写入链路标准

```text
应用日志
  → Agent / OTel Collector / Logstash / 自研采集器
  → 解析与 ECS 标准化
  → PII 脱敏
  → message / stack_trace 归一化
  → fingerprint 生成 event.hash
  → 重复日志窗口合并
  → Bulk API 写入 Elasticsearch data stream
  → ILM 自动 rollover / warm / cold / delete
```

---

## 14. 结论

超大型企业使用 Elasticsearch 存储应用日志时，核心不是“把日志写进 Elasticsearch”，而是建立受控的数据模型。官方文档已经明确指出，data stream 适合日志等持续生成的时间序列数据，ILM 可自动管理 rollover、保留与删除，ECS 提供日志字段命名与数据类型规范，`keyword`、`text`、`wildcard`、`flattened` 各自具有明确适用边界。([Elastic][1])

基于这些事实，超大型企业日志平台的最佳实践可以归纳为：使用 data stream 管理日志写入；使用 index template 固化 mapping 与 settings；使用 ECS 规范化文档结构；将可聚合字段设置为 keyword；将日志正文与异常消息设置为文本检索字段；将异常栈设置为 wildcard；将动态业务字段设置为 flattened；对重复日志和异常风暴在写入前做 fingerprint 与窗口合并；对多租户采用共享 data stream 与大租户拆分相结合；对大流量应用建立独立 data stream 与生命周期；对超长日志采用截断、hash、样本与外部对象存储结合的方式。

这套设计的关键判断是：**Elasticsearch 应保存可检索、可聚合、可诊断的数据，而不是无约束保存每一条原始字节流。**

[1]: https://www.elastic.co/docs/manage-data/data-store/data-streams "Data streams | Elastic Docs"
[2]: https://www.elastic.co/docs/solutions/observability/apm/data-streams?utm_source=chatgpt.com "APM data streams | Elastic Docs"
[3]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/dynamic "dynamic | Elasticsearch Reference"
[4]: https://www.elastic.co/docs/reference/elasticsearch/index-settings/mapping-limit "Mapping limit settings | Elasticsearch Reference"
[5]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/flattened "Flattened field type | Elasticsearch Reference"
[6]: https://www.elastic.co/docs/manage-data/lifecycle/index-lifecycle-management "Index lifecycle management (ILM) in Elasticsearch | Elastic Docs"
[7]: https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/size-shards "Size your shards | Elastic Docs"
[8]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/keyword "Keyword type family | Elasticsearch Reference"
[9]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/field-data-types "Field data types | Elasticsearch Reference"
[10]: https://www.elastic.co/docs/reference/ecs/ecs-error "Error fields | Elastic Common Schema (ECS)"
[11]: https://www.elastic.co/docs/reference/ecs "Elastic Common Schema (ECS) reference | Elastic Common Schema (ECS)"
[12]: https://www.elastic.co/docs/reference/ecs/ecs-event "Event fields | Elastic Common Schema (ECS)"
[13]: https://www.elastic.co/docs/reference/ecs/ecs-log "Log fields | Elastic Common Schema (ECS)"
[14]: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk "
  Bulk index or delete documents
 \| Elasticsearch API documentation
"
[15]: https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed "Tune for indexing speed | Elastic Docs"
[16]: https://www.elastic.co/docs/reference/enrich-processor/fingerprint-processor "Fingerprint processor | Elasticsearch Reference"
[17]: https://www.elastic.co/docs/manage-data/lifecycle/index-lifecycle-management/tutorial-time-series-without-data-streams "Manage time series data without data streams | Elastic Docs"
[18]: https://www.elastic.co/docs/manage-data/lifecycle/data-tiers "Elasticsearch data tiers: hot, warm, cold, and frozen storage explained | Elastic Docs"
[19]: https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/ignore-above?utm_source=chatgpt.com "ignore_above | Elasticsearch Reference"
[20]: https://www.elastic.co/docs/reference/aggregations/search-aggregations-bucket-terms-aggregation "Terms aggregation | Elasticsearch Reference"
[21]: https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results "Paginate search results | Elasticsearch Reference"
