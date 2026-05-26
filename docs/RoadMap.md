# RoadMap: StellSpec 企业级日志摄取代码实现步骤

## 1. Problem analysis

当前 `stellspec-service` 已具备基础链路：

```text
Stellflow 消费
  -> 日志归一化
  -> 单条 IndexRequest 写入 Elaticsearch
  -> 日索引 stellspec-logs-yyyy.MM.dd
```

该实现适合最小可运行版本，但不满足 `Draft.md` 中面向大型企业日志平台的要求。目标设计需要升级为：

```text
Stellflow 批量消费
  -> 标准化 ECS 文档
  -> 日志分类与路由
  -> 过滤、脱敏、归一化
  -> fingerprint 与窗口合并
  -> Bulk API 写入 data stream
  -> index template + lifecycle policy 管理 rollover / warm / cold / delete
```

核心变化：

- 从普通 index 写入升级为 data stream 写入。
- 从单条写入升级为 bulk 写入。
- 从“每条原始消息直接落库”升级为“过滤、归一化、合并后落库”。
- 从动态索引创建升级为 index template、component template、lifecycle policy 的显式治理。
- 从按天删除索引升级为 data stream lifecycle / ILM 删除 backing index。
- 从自定义字段模型逐步对齐 ECS。

## 2. Design

### 2.1 目标数据模型

日志文档统一采用 ECS 风格命名，并保留 StellSpec 扩展字段。

目标核心字段：

| 字段 | 类型方向 | 说明 |
| --- | --- | --- |
| `@timestamp` | `date` 或 `date_nanos` | 日志事件时间，data stream 必填 |
| `message` | `match_only_text` | 日志正文 |
| `log.level` | `keyword` | 日志级别 |
| `log.logger` | `keyword` | logger 名称 |
| `service.name` | `keyword` | 服务名 |
| `service.version` | `keyword` | 服务版本 |
| `service.environment` | `keyword` | 环境 |
| `event.dataset` | `keyword` | data stream dataset |
| `event.kind` | `keyword` | `event` 或 `metric` |
| `event.category` | `keyword` array | `application`、`web`、`database` 等 |
| `event.type` | `keyword` array | `info`、`error`、`access`、`audit`、`debug` 等 |
| `event.hash` | `keyword` | 归一化日志 fingerprint |
| `trace.id` | `keyword` | Trace ID |
| `span.id` | `keyword` | Span ID |
| `tenant.id` | `keyword` | 租户 |
| `error.type` | `keyword` | 异常类型 |
| `error.message` | `match_only_text` | 异常消息 |
| `error.stack_trace` | `wildcard` | 完整异常栈 |
| `labels` | `flattened` | 低风险标签 |
| `attributes` | `flattened` | 动态业务属性 |
| `stellspec.ingest.*` | keyword/long/date | 摄取元数据 |
| `stellflow.*` | keyword/integer/long | Stellflow 来源元数据 |

### 2.2 Data stream 命名

默认命名遵循：

```text
logs-<dataset>-<namespace>
```

示例：

```text
logs-order-service-prod
logs-order-service-error-prod
logs-gateway-access-prod
logs-audit-prod
logs-stellspec-aggregate-prod
```

命名规则：

- `logs`：固定 type。
- `dataset`：优先由日志分类、服务名、日志类型决定。
- `namespace`：环境、租户隔离域或区域，例如 `prod`、`tenant-a`、`cn-prod`。
- 大流量应用、审计日志、错误日志、debug 日志需要独立 data stream。

### 2.3 写入语义

data stream 写入采用 append-only 语义：

- 正常日志使用 Bulk API 的 `create` action 写入 data stream。
- 不依赖相同 `_id` 覆盖旧文档。
- 删除通过 lifecycle policy 删除 backing index。
- 更新或删除单条日志不是默认能力，只能作为受审计的管理能力访问 backing index。

重复日志不通过“相同 `_id` 后写覆盖”解决，而是在写入前通过 fingerprint 和时间窗口合并。

## 3. Implementation

### Phase 1: 配置模型与目录整理

目标：先把企业级能力所需配置落到稳定的 Spring Boot properties 中。

代码步骤：

1. 新增 `StellspecElaticsearchProperties`。
   - 路径：`src/main/java/io/github/stellhub/stellspec/service/config/StellspecElaticsearchProperties.java`
   - 配置项：data stream 前缀、namespace、template 名称、lifecycle policy 名称、bootstrap 开关、template overwrite 开关。

2. 扩展 `StellspecLogProperties`。
   - 增加 `datasetDefault`、`namespaceDefault`、`classificationRules`、`filterRules`、`mergeRules`、`bulk`、`retentionPolicies`。
   - 保留旧 `indexPrefix` 作为兼容字段，标记为过渡配置。

3. 新增配置结构类。
   - `BulkWriteProperties`
   - `LogClassificationProperties`
   - `LogFilterProperties`
   - `LogMergeProperties`
   - `RetentionPolicyProperties`

4. 更新 `application.yaml`。
   - 增加默认 data stream 配置。
   - 增加 bulk flush 条件。
   - 增加普通日志、错误日志、access log、audit log、debug log 的保留策略。

验收标准：

- `@ConfigurationProperties` 可以完整绑定配置。
- 单元测试覆盖默认值和 YAML 绑定。

### Phase 2: ECS 文档模型重构

目标：把当前 `LogDocument` 从扁平字段升级为 ECS 风格文档。

代码步骤：

1. 新增 ECS 结构化文档模型。
   - `EcsLogDocument`
   - `EcsService`
   - `EcsLog`
   - `EcsEvent`
   - `EcsTrace`
   - `EcsSpan`
   - `EcsError`
   - `EcsTenant`
   - `StellspecIngest`
   - `StellflowSource`

2. 将 `LogPayloadNormalizer` 拆分为多段处理。
   - `OtelLogPayloadParser`：解析 OpenTelemetry logs JSON。
   - `PlainLogPayloadParser`：解析平铺 JSON 和普通文本。
   - `EcsLogDocumentMapper`：将解析结果映射为 ECS 文档。
   - `ExceptionFieldExtractor`：提升 `exception.*` 到 `error.*`。

3. 保留原始 payload 策略。
   - 短日志可保留在 `stellspec.ingest.raw_payload`。
   - 超长日志只保留 preview、hash、length。
   - 后续对象存储能力预留 `external.storage` 和 `external.object_key`。

4. 实现字段规范化。
   - `severityText` -> `log.level`
   - `serviceName` / `resource.service.name` -> `service.name`
   - `traceId` -> `trace.id`
   - `spanId` -> `span.id`
   - `body` -> `message`
   - `exception.type` -> `error.type`
   - `exception.message` -> `error.message`
   - `exception.stacktrace` -> `error.stack_trace`

验收标准：

- 平铺 JSON、OTel `resourceLogs`、普通文本都能转换为 ECS 文档。
- 异常栈在同一个文档中保存，不拆分成多文档。
- 单元测试覆盖普通日志、错误日志、异常栈、超长日志。

### Phase 3: 日志分类与 data stream 路由

目标：按大型企业日志分类策略决定 data stream，而不是全部写入一个索引。

代码步骤：

1. 新增 `LogCategory` 枚举。
   - `APPLICATION`
   - `ERROR`
   - `ACCESS`
   - `AUDIT`
   - `DEBUG`
   - `SLOW_LOG`
   - `SECURITY`
   - `AGGREGATE`

2. 新增 `LogClassifier`。
   - 根据 `log.level`、`error.*`、HTTP 字段、audit 字段、logger 名称、attributes 标签分类。
   - 输出 `LogClassificationResult`，包含 category、dataset、namespace、retention tier。

3. 新增 `DataStreamNameResolver`。
   - 输入 ECS 文档和分类结果。
   - 输出 `logs-<dataset>-<namespace>`。
   - 大流量应用支持配置级覆写，例如 `gateway-access` 独立 data stream。

4. 修改写入入口。
   - 当前 `LogDocument.indexName` 迁移为 `EcsLogDocument.dataStreamName`。
   - 写入层不再拼接日索引名。

验收标准：

- ERROR 日志进入 error data stream。
- ACCESS 日志进入 access data stream。
- AUDIT 日志进入 audit data stream。
- DEBUG 日志进入短生命周期 data stream。
- 默认应用日志进入 `logs-<service.name>-<namespace>`。

### Phase 4: Index template、component template 与 lifecycle policy

目标：服务启动时显式创建或校验 data stream 所需模板和生命周期策略。

代码步骤：

1. 新增模板资源目录。
   - `src/main/resources/elaticsearch/component-templates/stellspec-ecs-mappings.json`
   - `src/main/resources/elaticsearch/component-templates/stellspec-log-settings.json`
   - `src/main/resources/elaticsearch/index-templates/stellspec-logs-template.json`
   - `src/main/resources/elaticsearch/lifecycle/stellspec-logs-default-policy.json`
   - `src/main/resources/elaticsearch/lifecycle/stellspec-logs-short-policy.json`
   - `src/main/resources/elaticsearch/lifecycle/stellspec-logs-audit-policy.json`

2. 新增 `ElaticsearchTemplateManager`。
   - 创建/更新 component template。
   - 创建/更新 index template。
   - 创建/更新 lifecycle policy。
   - 校验 `@timestamp` mapping 和 `data_stream` 配置。

3. 新增 `ElaticsearchBootstrapRunner`。
   - 在应用启动后执行 bootstrap。
   - 支持 `validate-only`、`create-if-absent`、`overwrite` 三种模式。
   - 生产默认 `validate-only` 或 `create-if-absent`，避免误覆盖线上模板。

4. 定义 template mapping。
   - `dynamic: false`
   - `labels` / `attributes` 使用 `flattened`
   - `message` 使用 `match_only_text`
   - `error.stack_trace` 使用 `wildcard`
   - 聚合字段使用 `keyword`
   - 设置 `index.mapping.total_fields.limit`
   - 设置 `index.mapping.ignore_above`

5. 定义 lifecycle policy。
   - default：hot rollover，warm forcemerge，cold 可选，delete。
   - short：debug/access 高频短保留。
   - audit：长保留或专用 data stream。

验收标准：

- 启动时能创建 data stream 所需 template 和 lifecycle policy。
- 禁用自动创建索引时，服务仍可写入已匹配模板的 data stream。
- 集成测试校验 template JSON 的关键字段存在。

### Phase 5: Bulk API 写入层

目标：替换单条 `IndexRequest`，使用 Bulk API 批量写入 data stream。

代码步骤：

1. 新增 `BulkLogWriter` 接口。
   - `write(List<RoutedLogDocument> documents)`
   - 返回 `BulkWriteResult`，包含成功数、失败数、失败明细。

2. 新增 `ElaticsearchBulkLogWriter`。
   - 使用 Elasticsearch Java API Client 的 bulk API。
   - 对 data stream 使用 `create` action。
   - 不使用 `index` 覆盖语义。

3. 新增 `BulkLogBuffer`。
   - 按 max actions、max bytes、flush interval 触发 flush。
   - 支持 graceful shutdown flush。
   - 支持背压：队列满时阻塞或拒绝，策略可配置。

4. 新增 `BulkFailureHandler`。
   - 区分可重试错误和不可重试错误。
   - 429、503、timeout 进入重试。
   - mapping error、validation error 进入 dead letter。
   - 记录失败样本和错误摘要。

5. 新增 `DeadLetterLogWriter`。
   - 写入 `logs-stellspec-deadletter-<namespace>` data stream。
   - 保存原始 payload、错误原因、解析阶段、写入阶段。

6. 暴露 bulk 指标。
   - flush 次数
   - bulk size
   - success count
   - failure count
   - retry count
   - dead letter count
   - queue depth
   - flush latency

验收标准：

- 默认写入路径不再逐条调用 `index`。
- bulk 失败不会静默丢日志。
- 应用关闭时 buffer 内日志被 flush 或明确进入 dead letter。

### Phase 6: Stellflow 消费与 offset 提交重构

目标：保证 offset 提交发生在 bulk 写入成功之后。

代码步骤：

1. 评估当前 `@StellflowListener` 模式。
   - 当前 listener 方法按单条消息回调。
   - 如果方法返回后框架自动提交 offset，则不适合异步 bulk 后提交。

2. 新增 `StellflowBulkConsumerWorker`。
   - 直接使用 Stellflow consumer 拉取批次。
   - poll 一批消息。
   - 归一化、分类、过滤、合并。
   - bulk 写入成功后提交 offset。
   - bulk 写入失败时不提交或按 dead letter 策略提交。

3. 保留 `StellflowLogListener` 作为兼容入口。
   - 默认关闭或仅用于低吞吐开发模式。
   - 生产模式使用 `StellflowBulkConsumerWorker`。

4. 增加消费并发模型。
   - 按 topic/partition 分配 worker。
   - 每个 partition 内保持 offset 有序。
   - 不同 partition 可并行 bulk。

5. 增加 offset 策略配置。
   - `commit-after-bulk-success`
   - `commit-after-deadletter`
   - `max-uncommitted-records`
   - `poll-timeout`

验收标准：

- bulk 成功前不提交 offset。
- 单 partition 内不会因为异步 flush 导致 offset 越序提交。
- 写入失败重启后可重新消费未提交消息。

### Phase 7: 日志过滤、脱敏与超长日志处理

目标：在写入前控制数据质量和成本。

代码步骤：

1. 新增 `LogFilterChain`。
   - `LevelFilter`
   - `LoggerNameFilter`
   - `TenantFilter`
   - `HealthCheckAccessLogFilter`
   - `DebugSamplingFilter`
   - `OversizedLogFilter`

2. 新增 `LogRedactor`。
   - 支持正则脱敏。
   - 支持字段路径脱敏，例如 `attributes.password`、`headers.Authorization`。
   - 支持 hash 保留，例如手机号、邮箱。

3. 新增 `LargeLogHandler`。
   - 计算 `message_length`。
   - 超长日志生成 `message_preview` 和 `message_hash`。
   - 保留完整 `error.stack_trace` 的策略单独配置。
   - 预留对象存储接口 `ExternalPayloadStore`。

4. 新增过滤结果字段。
   - 被采样：`stellspec.ingest.sampled`
   - 被截断：`stellspec.ingest.truncated`
   - 原始长度：`stellspec.ingest.original_length`
   - 处理策略：`stellspec.ingest.policy`

验收标准：

- 健康检查 access log 可被过滤。
- debug 日志可按比例采样。
- 敏感字段不会进入最终文档。
- 超长日志不会导致 bulk 请求过大。

### Phase 8: 重复日志 fingerprint 与窗口合并

目标：在异常风暴和重复日志场景下减少写入量，保留诊断信息和规模信息。

代码步骤：

1. 新增 `LogFingerprintGenerator`。
   - 输入 ECS 文档。
   - 对 message 归一化：数字、UUID、traceId、订单号等替换为占位符。
   - 对 stack trace 归一化：保留异常类型、根因、关键 top frame。
   - 输出 `event.hash`。

2. 新增 `LogMergeWindowAggregator`。
   - 使用时间窗口合并相同 `event.hash`。
   - 记录 `first_seen`、`last_seen`、`occurrence_count`。
   - 保留首条样本、最近样本、示例 trace id。

3. 新增聚合文档模型。
   - `AggregatedLogDocument`
   - `event.kind = metric`
   - `event.hash`
   - `stellspec.merge.window_start`
   - `stellspec.merge.window_end`
   - `stellspec.merge.occurrence_count`
   - `stellspec.merge.sample_message`
   - `stellspec.merge.sample_stack_trace`

4. 新增样本保留策略。
   - 第一条完整保留。
   - 每窗口最近一条完整保留。
   - 聚合文档写入 aggregate data stream。
   - 高频重复日志按配置只写样本和聚合计数。

5. 新增 flush 机制。
   - 窗口到期 flush。
   - 应用关闭 flush。
   - 内存阈值触发 flush。

验收标准：

- 同一窗口内相同异常不会重复写入完整 stacktrace。
- 聚合文档能反映真实 occurrence count。
- 样本文档保留可诊断信息。
- 合并逻辑不会跨 tenant、service、environment 错误合并。

### Phase 9: 删除策略与生命周期治理

目标：遵循 data stream 生命周期，而不是对日志执行 CRUD 式逐条删除。

代码步骤：

1. 定义保留策略枚举。
   - `SHORT`
   - `DEFAULT`
   - `ERROR`
   - `AUDIT`
   - `SECURITY`
   - `AGGREGATE`

2. 新增 `RetentionPolicyResolver`。
   - 根据日志分类、租户、服务、环境决定 lifecycle policy。
   - 输出 data stream template 选择或 namespace 策略。

3. 增加 lifecycle policy JSON。
   - short：1-7 天。
   - default：7-30 天。
   - error：30-90 天。
   - audit/security：按合规要求长期保留。
   - aggregate：长于样本日志，便于趋势分析。

4. 明确删除约束。
   - 默认删除由 lifecycle policy 删除 backing index。
   - 禁止业务接口提供任意 `delete_by_query`。
   - 管理员删除必须审计，且优先按 data stream / backing index / 时间范围执行。

5. 新增 `LogDeletionPolicyDocument`。
   - 文档化每类日志的保留周期。
   - 与配置绑定，避免代码和文档漂移。

验收标准：

- 每个 data stream 能映射到明确生命周期策略。
- 删除策略不依赖逐条 delete。
- 审计和安全日志不会被短周期策略误删。

### Phase 10: HTTP 管理与观测 API

目标：让企业运维能检查服务状态、模板状态、bulk 状态和消费状态。

代码步骤：

1. 扩展 `StellspecLogController`。
   - `GET /api/stellspec/logs/status`
   - `GET /api/stellspec/logs/data-streams`
   - `GET /api/stellspec/logs/templates/status`
   - `GET /api/stellspec/logs/bulk/status`
   - `POST /api/stellspec/logs/dry-run/normalize`
   - `POST /api/stellspec/logs/dry-run/route`

2. 新增 response model。
   - `TemplateStatusResponse`
   - `BulkStatusResponse`
   - `ConsumerStatusResponse`
   - `DryRunNormalizeResponse`
   - `DryRunRouteResponse`

3. 增加运维保护。
   - dry-run 不写 Elaticsearch。
   - template overwrite 默认不通过 HTTP 暴露。
   - 删除类管理能力不在第一阶段暴露。

验收标准：

- 本地可用 dry-run 验证一条日志会进入哪个 data stream。
- 可查看 bulk 队列和最近错误。
- 可查看 template/lifecycle bootstrap 状态。

### Phase 11: 测试策略

目标：覆盖企业级链路，不只测单个 normalizer。

代码步骤：

1. 单元测试。
   - `EcsLogDocumentMapperTest`
   - `ExceptionFieldExtractorTest`
   - `DataStreamNameResolverTest`
   - `LogClassifierTest`
   - `LogFilterChainTest`
   - `LogFingerprintGeneratorTest`
   - `LogMergeWindowAggregatorTest`

2. 集成测试。
   - 使用 Testcontainers Elasticsearch 或本地 profile。
   - 校验 component template、index template、lifecycle policy 创建。
   - 校验 bulk 写入 data stream。
   - 校验 `@timestamp` 必填。
   - 校验 mapping 中 `attributes` / `labels` 为 `flattened`。

3. 消费链路测试。
   - Stellflow mock consumer 或 mini broker。
   - bulk 成功后提交 offset。
   - bulk 失败不提交 offset。
   - dead letter 后按策略提交。

4. 性能基准。
   - bulk size: 100、200、500、1000 梯度测试。
   - flush interval: 1s、3s、5s 测试。
   - 合并前后写入量对比。

验收标准：

- `mvn test` 覆盖核心纯逻辑。
- 集成 profile 能验证真实 Elaticsearch data stream 写入。
- 性能测试产出推荐 bulk 参数。

### Phase 12: 迁移与兼容

目标：从当前单条 index 写入平滑迁移到企业级 data stream 写入。

代码步骤：

1. 保留旧写入器一段时间。
   - `SingleIndexLogWriter`
   - `BulkDataStreamLogWriter`
   - 通过配置选择。

2. 默认开发环境可使用旧模式。
   - 便于无 template 的本地快速验证。
   - 文档标记为 dev-only。

3. 生产默认使用 data stream。
   - 如果 template 校验失败，启动失败或进入只读保护模式。
   - 不允许静默退回动态 mapping。

4. 迁移 README 和 ADR。
   - README 说明 data stream 配置。
   - ADR 更新最终决策。
   - Draft 中的标准转为工程配置说明。

验收标准：

- 本地开发仍可快速启动。
- 生产配置不依赖自动创建普通 index。
- 文档、配置和代码行为一致。

## 4. Complete code

建议最终代码结构如下：

```text
src/main/java/io/github/stellhub/stellspec/service
  config/
    StellspecLogProperties.java
    StellspecElaticsearchProperties.java
    BulkWriteProperties.java
    LogClassificationProperties.java
    LogFilterProperties.java
    LogMergeProperties.java
    RetentionPolicyProperties.java
  log/
    consumer/
      StellflowBulkConsumerWorker.java
      StellflowConsumerOffsetManager.java
    domain/
      EcsLogDocument.java
      EcsService.java
      EcsLog.java
      EcsEvent.java
      EcsTrace.java
      EcsSpan.java
      EcsError.java
      EcsTenant.java
      StellspecIngest.java
      StellflowSource.java
      RoutedLogDocument.java
      AggregatedLogDocument.java
    normalize/
      OtelLogPayloadParser.java
      PlainLogPayloadParser.java
      EcsLogDocumentMapper.java
      ExceptionFieldExtractor.java
      LargeLogHandler.java
    classify/
      LogCategory.java
      LogClassifier.java
      DataStreamNameResolver.java
      RetentionPolicyResolver.java
    filter/
      LogFilter.java
      LogFilterChain.java
      LevelFilter.java
      LoggerNameFilter.java
      DebugSamplingFilter.java
      OversizedLogFilter.java
      LogRedactor.java
    merge/
      LogFingerprintGenerator.java
      LogMergeWindowAggregator.java
      MergeWindowState.java
    writer/
      BulkLogWriter.java
      ElaticsearchBulkLogWriter.java
      BulkLogBuffer.java
      BulkFailureHandler.java
      DeadLetterLogWriter.java
    template/
      ElaticsearchTemplateManager.java
      ElaticsearchBootstrapRunner.java
  web/
    StellspecLogController.java
    TemplateStatusResponse.java
    BulkStatusResponse.java
    ConsumerStatusResponse.java
    DryRunNormalizeResponse.java
    DryRunRouteResponse.java

src/main/resources/elaticsearch
  component-templates/
    stellspec-ecs-mappings.json
    stellspec-log-settings.json
  index-templates/
    stellspec-logs-template.json
  lifecycle/
    stellspec-logs-default-policy.json
    stellspec-logs-short-policy.json
    stellspec-logs-error-policy.json
    stellspec-logs-audit-policy.json
```

推荐实现顺序：

1. 配置模型与 ECS 文档模型。
2. 日志分类与 data stream 路由。
3. index template 与 lifecycle bootstrap。
4. bulk writer 与失败处理。
5. Stellflow 批量消费和 offset 提交。
6. 过滤、脱敏和超长日志处理。
7. fingerprint 与窗口合并。
8. 删除策略和保留策略固化。
9. HTTP 管理 API。
10. 集成测试和性能参数校准。

第一阶段最小可交付范围：

- ECS 文档模型。
- Data stream name resolver。
- Index template + lifecycle bootstrap。
- Bulk API 写入。
- 基础分类：application / error / access / audit / debug。
- 基础过滤：debug sampling、health check access log drop。
- 基础合并：同一窗口内相同 `event.hash` 的异常聚合。

完成第一阶段后，服务应从当前：

```text
单条消息 -> 单条 IndexRequest -> 普通日索引
```

升级为：

```text
批量消息 -> 标准化/分类/过滤/合并 -> Bulk create -> logs-<dataset>-<namespace> data stream
```
