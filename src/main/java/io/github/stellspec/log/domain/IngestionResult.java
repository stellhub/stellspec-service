package io.github.stellspec.log.domain;

import java.time.Instant;

/** 日志写入结果。 */
public record IngestionResult(
        String id,
        String indexName,
        String topic,
        int partition,
        long offset,
        Instant eventTime,
        String result) {}
