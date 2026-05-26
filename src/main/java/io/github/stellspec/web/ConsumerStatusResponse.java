package io.github.stellspec.web;

import java.time.Instant;
import java.util.Map;

/** Stellflow consumer 状态响应。 */
public record ConsumerStatusResponse(
        boolean enabled,
        boolean running,
        int inFlightRecords,
        int estimatedLagRecords,
        long pollCount,
        long polledRecordCount,
        long processedRecordCount,
        long filteredRecordCount,
        long committedOffsetCount,
        long commitFailureCount,
        long backpressurePauseCount,
        Instant lastPollAt,
        Instant lastCommitAt,
        Instant lastFailureAt,
        String lastError,
        Map<String, Long> committedOffsets) {}
