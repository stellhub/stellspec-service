package io.github.stellspec.log.writer;

import io.github.stellspec.log.domain.StellflowSource;
import java.util.List;

/** Bulk 失败处理结果。 */
public record BulkFailureHandlingResult(
        int initialFailureCount,
        int retriedCount,
        int retrySuccessCount,
        int deadLetterCount,
        int deadLetterFailureCount,
        int unresolvedFailureCount,
        List<StellflowSource> unresolvedSources) {

    public BulkFailureHandlingResult {
        unresolvedSources = unresolvedSources == null ? List.of() : List.copyOf(unresolvedSources);
    }

    public BulkFailureHandlingResult(
            int initialFailureCount,
            int retriedCount,
            int retrySuccessCount,
            int deadLetterCount,
            int deadLetterFailureCount,
            int unresolvedFailureCount) {
        this(
                initialFailureCount,
                retriedCount,
                retrySuccessCount,
                deadLetterCount,
                deadLetterFailureCount,
                unresolvedFailureCount,
                List.of());
    }

    /**
     * 是否所有失败都已经通过重试或 dead letter 得到处理。
     *
     * @return 是否已处理
     */
    public boolean fullyHandled() {
        return unresolvedFailureCount == 0 && deadLetterFailureCount == 0;
    }

    /**
     * 创建空处理结果。
     *
     * @return 空结果
     */
    public static BulkFailureHandlingResult empty() {
        return new BulkFailureHandlingResult(0, 0, 0, 0, 0, 0, List.of());
    }
}
