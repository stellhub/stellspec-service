# stellspec-service 部署说明

这个压缩包包含可直接运行的 Spring Boot Jar，以及建议外置管理的运行配置文件。

## 文件说明

- `stellspec-service.jar`：可执行 Spring Boot Jar。
- `application.yaml`：应用运行配置，包括 Stellflow、Elaticsearch、日志摄取、过滤、合并和消费者配置。
- `logback.xml`：服务自身日志配置，默认同时输出到 console 和本地滚动文件。
- `README.md`：当前部署说明。

## 快速启动

在解压目录中执行：

```bash
java -jar stellspec-service.jar \
  --spring.config.location=optional:classpath:/,file:./application.yaml \
  --logging.config=./logback.xml
```

这条命令会保留 Jar 内置配置作为兜底，同时让当前目录下的 `application.yaml` 覆盖运行参数。
`logback.xml` 会从当前目录加载，因此可以独立调整服务自身日志的 console 和本地文件输出策略。

## 常用运行参数

可以直接编辑 `application.yaml`，也可以在启动前通过环境变量覆盖常用配置：

```bash
export SERVER_PORT=18090
export STELLFLOW_BOOTSTRAP_SERVERS=127.0.0.1:9092
export STELLSPEC_STELLFLOW_LOG_TOPIC=stello11y.logs.app.prod.v1
export STELLSPEC_STELLFLOW_GROUP_ID=stellspec-service
export ELATICSEARCH_ENDPOINT=http://127.0.0.1:9200

java -jar stellspec-service.jar \
  --spring.config.location=optional:classpath:/,file:./application.yaml \
  --logging.config=./logback.xml
```

## 日志配置

默认情况下，`logback.xml` 会把服务自身日志写入：

```text
logs/stellspec-service.log
logs/archive/
```

常用覆盖项：

```bash
export STELLSPEC_LOG_DIR=/data/stellspec-service/logs
export STELLSPEC_ROOT_LOG_LEVEL=INFO
export STELLSPEC_APP_LOG_LEVEL=INFO
export STELLSPEC_ES_WRITE_INFO_SUMMARY_INTERVAL_MILLIS=10000
```

这个服务本身是日志消费应用，默认应保持 `stellflux.opentelemetry.logs.enabled=false`，
避免把服务自身日志再次发送回日志消费链路。

## 健康检查

启动后可以检查服务状态：

```bash
curl http://127.0.0.1:18090/api/stellspec/logs/status
```

如果启用了独立 bulk consumer，也可以检查消费者状态：

```bash
curl http://127.0.0.1:18090/api/stellspec/logs/consumer/status
```
