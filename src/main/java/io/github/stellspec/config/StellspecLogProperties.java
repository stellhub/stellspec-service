package io.github.stellspec.config;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** StellSpec 日志写入配置。 */
@Getter
@Setter
@ConfigurationProperties(prefix = "stellspec.logs")
public class StellspecLogProperties {

    private static final DateTimeFormatter INDEX_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy.MM.dd");

    /** Elaticsearch 日志索引前缀。 */
    private String indexPrefix = "stellspec-logs";

    /** 索引日期时区。 */
    private String indexZone = "UTC";

    /** 无法从日志资源中解析服务名时使用的兜底服务名。 */
    private String fallbackServiceName = "unknown-service";

    /** 是否保留原始 Stellflow 消息体。 */
    private boolean includeRawPayload = true;

    /** 默认 data stream dataset。 */
    private String datasetDefault = "application";

    /** 默认 data stream namespace。 */
    private String namespaceDefault = "prod";

    /** 默认 data stream type。 */
    private String dataStreamType = "logs";

    /** 兜底环境名称。 */
    private String defaultEnvironment = "prod";

    /** 最大 message 长度，超过后写入 preview 与 hash。 */
    private int maxMessageLength = 16_384;

    /** 最大原始 payload 长度，超过后不直接写入原文。 */
    private int maxRawPayloadLength = 65_536;

    /** Bulk 写入配置。 */
    private Bulk bulk = new Bulk();

    /** 过滤配置。 */
    private Filter filter = new Filter();

    /** 合并配置。 */
    private Merge merge = new Merge();

    /** 日志分类配置。 */
    private Classification classification = new Classification();

    /** 独立 Stellflow 批量消费配置。 */
    private BulkConsumer bulkConsumer = new BulkConsumer();

    /**
     * 根据日志时间生成索引名。
     *
     * @param eventTime 日志事件时间
     * @return 索引名称
     */
    public String indexName(Instant eventTime) {
        Instant safeEventTime = eventTime == null ? Instant.now() : eventTime;
        String date = INDEX_DATE_FORMATTER.withZone(ZoneId.of(indexZone)).format(safeEventTime);
        return indexPrefix + "-" + date;
    }

    /** Bulk 写入配置。 */
    @Getter
    @Setter
    public static class Bulk {

        /** 单次 bulk 最大文档数。 */
        private int maxActions = 500;

        /** 单次 bulk 最大估算字节数。 */
        private long maxBytes = 5 * 1024 * 1024L;

        /** 定时 flush 间隔，单位毫秒。 */
        private long flushIntervalMillis = 3000L;

        /** Bulk item 可重试失败的最大重试次数。 */
        private int maxRetries = 2;

        /** Bulk item 重试间隔，单位毫秒。 */
        private long retryBackoffMillis = 500L;
    }

    /** 日志过滤配置。 */
    @Getter
    @Setter
    public static class Filter {

        /** 是否丢弃健康检查 access log。 */
        private boolean dropHealthCheckAccessLogs = true;

        /** debug 日志采样比例，1 表示全量保留。 */
        private double debugSampleRate = 0.1D;

        /** 健康检查路径。 */
        private List<String> healthCheckPaths = new ArrayList<>(List.of("/health", "/actuator/health"));

        /** 需要脱敏的字段名片段。 */
        private List<String> sensitiveFieldNames =
                new ArrayList<>(List.of("password", "authorization", "token", "secret"));
    }

    /** 重复日志合并配置。 */
    @Getter
    @Setter
    public static class Merge {

        /** 是否启用窗口合并。 */
        private boolean enabled = true;

        /** 合并窗口毫秒数。 */
        private long windowMillis = 60_000L;

        /** 同一窗口内超过该次数后只输出聚合文档。 */
        private int duplicateThreshold = 3;
    }

    /** 日志分类配置。 */
    @Getter
    @Setter
    public static class Classification {

        /** access log dataset 后缀。 */
        private String accessDatasetSuffix = "access";

        /** error log dataset 后缀。 */
        private String errorDatasetSuffix = "error";

        /** audit log dataset。 */
        private String auditDataset = "audit";

        /** debug log dataset 后缀。 */
        private String debugDatasetSuffix = "debug";

        /** 聚合日志 dataset。 */
        private String aggregateDataset = "stellspec-aggregate";
    }

    /** 独立批量消费配置。 */
    @Getter
    @Setter
    public static class BulkConsumer {

        /** 是否启用独立批量消费 worker。 */
        private boolean enabled = false;

        /** 单次 poll 超时时间，单位毫秒。 */
        private long pollTimeoutMillis = 3000L;

        /** Partition worker 线程数上限。 */
        private int partitionWorkerThreads = 4;

        /** 最大未提交记录数，超过后暂停 poll 形成背压。 */
        private int maxUncommittedRecords = 10_000;

        /** 触发背压后的暂停时间，单位毫秒。 */
        private long backpressureSleepMillis = 200L;

        /** 消费失败后的暂停时间，单位毫秒。 */
        private long failureBackoffMillis = 1000L;

        /** Dead letter 写入成功后是否提交 offset。 */
        private boolean commitAfterDeadLetter = true;

        /** Dead letter data stream 名称。 */
        private String deadLetterDataStream = "logs-stellspec-deadletter-prod";
    }
}
