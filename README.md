# StellSpec Service

`stellspec-service` 是 StellSpec 可观测性链路中的服务端组件，负责接收、规范化并写入日志数据，为后续查询和控制台展示提供数据基础。

## 项目概述

该服务位于采集链路中游，承接上游日志消息，处理日志正文、级别、时间戳、资源属性和链路上下文，然后写入搜索存储。

## 当前状态

| 项目 | 说明 |
| --- | --- |
| 稳定性 | 开发中 |
| 服务类型 | 日志处理服务 |
| 推荐运行时 | Java 25 |
| 推荐框架 | Spring Boot 3.x、Stellflux |
| 维护方 | StellHub |

## 解决什么问题

- 消费上游日志消息。
- 标准化日志字段和运行时属性。
- 保留链路上下文，便于问题排查。
- 写入搜索存储，支撑控制台查询。
- 暴露健康检查和运行指标。

## 不解决什么问题

- 不负责日志采集代理本身。
- 不提供前端查询界面。
- 不替代底层搜索存储。
- 不直接承担业务侧日志 SDK 职责。

## 核心能力

| 能力 | 说明 | 典型场景 |
| --- | --- | --- |
| 消息消费 | 读取日志事件 | 日志处理 |
| 字段标准化 | 统一字段和属性 | 检索与聚合 |
| 上下文保留 | 保留 trace/resource 信息 | 链路排查 |
| 写入存储 | 写入搜索后端 | 查询分析 |
| 健康检查 | 暴露服务状态 | 运维巡检 |

## 架构说明

```mermaid
flowchart LR
    Collector[Collector] --> Flow[Stellflow]
    Flow --> Service[StellSpec Service]
    Service --> Store[Search Storage]
    Store --> Console[StellSpec Console]
```

## 快速开始

```bash
mvn clean test
mvn clean package -DskipTests
mvn spring-boot:run
```

## 配置说明

| 配置项 | 是否必填 | 说明 |
| --- | --- | --- |
| server.port | 否 | HTTP 端口 |
| stellflow.consumer.topic | 是 | 消费主题 |
| search.endpoint | 是 | 搜索存储地址 |
| stellspec.batch.size | 否 | 批量写入大小 |

## 本地开发

```bash
mvn clean verify
```

涉及消费、批处理、字段映射和写入逻辑的改动必须补充测试。

## 版本与升级

- `MAJOR`：不兼容数据模型或写入结构变更。
- `MINOR`：向后兼容的新能力。
- `PATCH`：向后兼容的问题修复。

## 可观测性

| 类型 | 名称 | 说明 |
| --- | --- | --- |
| Metric | stellspec_ingest_total | 处理日志数量 |
| Metric | stellspec_write_failure_total | 写入失败数量 |
| Metric | stellspec_batch_latency | 批处理耗时 |
| Log | INGEST_FAILED | 处理失败 |
| Log | WRITE_RETRY | 写入重试 |

## 故障排查

### 日志没有写入存储

1. 检查消费主题是否正确。
2. 检查搜索存储地址是否可用。
3. 检查写入失败日志。
4. 检查消费位点是否推进。

## 安全说明

- 运行配置不应直接提交到仓库。
- 管理接口应限制访问范围。
- 生产环境遵守平台数据规范。

## 目录结构

```text
.
├── src/            # 服务源码
├── docs/           # 扩展文档
├── pom.xml         # Maven 构建文件
└── README.md       # 项目说明
```

## 贡献规范

- 数据结构变更必须说明兼容性影响。
- 消费、批处理和写入逻辑变更必须补充测试。
- 行为变更必须同步更新 README 或 docs。

## 支持

由 StellHub 维护。建议通过 GitHub Issues 记录问题、需求和设计讨论。

## 许可证

以仓库内 `LICENSE` 文件为准。