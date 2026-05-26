package io.github.stellspec.log.domain;

import java.util.List;
import java.util.stream.IntStream;

/** Bulk 写入结果。 */
public record BulkWriteResult(
        int successCount,
        int failureCount,
        List<String> failures,
        List<BulkItemFailure> itemFailures) {

    public BulkWriteResult(int successCount, int failureCount, List<String> failures) {
        this(successCount, failureCount, failures, List.of());
    }

    public BulkWriteResult {
        failures = failures == null ? List.of() : List.copyOf(failures);
        itemFailures = itemFailures == null ? List.of() : List.copyOf(itemFailures);
    }

    /**
     * 创建全成功结果。
     *
     * @param successCount 成功数量
     * @return Bulk 写入结果
     */
    public static BulkWriteResult success(int successCount) {
        return new BulkWriteResult(successCount, 0, List.of(), List.of());
    }

    /**
     * 创建整批失败结果。
     *
     * @param documents 请求写入文档
     * @param reason 失败原因
     * @return Bulk 写入结果
     */
    public static BulkWriteResult failedAll(List<RoutedLogDocument> documents, String reason) {
        List<RoutedLogDocument> safeDocuments = documents == null ? List.of() : documents;
        List<BulkItemFailure> failures =
                IntStream.range(0, safeDocuments.size())
                        .mapToObj(
                                index -> {
                                    RoutedLogDocument document = safeDocuments.get(index);
                                    return new BulkItemFailure(
                                            index,
                                            document.dataStreamName(),
                                            document.document().getId(),
                                            null,
                                            "io_exception",
                                            reason,
                                            BulkFailureType.RETRYABLE);
                                })
                        .toList();
        return new BulkWriteResult(
                0,
                safeDocuments.size(),
                failures.stream().map(BulkItemFailure::summary).toList(),
                failures);
    }
}
