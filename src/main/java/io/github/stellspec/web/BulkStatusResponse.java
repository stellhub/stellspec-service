package io.github.stellspec.web;

import java.time.Instant;

/** Bulk 写入状态响应。 */
public record BulkStatusResponse(
        int pendingCount,
        long flushCount,
        long successCount,
        long failureCount,
        long retryAttemptCount,
        long retryDocumentCount,
        long retrySuccessCount,
        long deadLetterCount,
        long deadLetterFailureCount,
        long unresolvedFailureCount,
        Instant lastFailureAt,
        String lastFailureSummary) {}
