package io.github.stellspec.log.domain;

/** Bulk item 级失败明细。 */
public record BulkItemFailure(
        int itemIndex,
        String dataStreamName,
        String documentId,
        Integer status,
        String errorType,
        String reason,
        BulkFailureType failureType) {

    /**
     * 当前失败是否允许重试。
     *
     * @return 是否可重试
     */
    public boolean retryable() {
        return failureType == BulkFailureType.RETRYABLE;
    }

    /**
     * 生成面向日志与 API 的简要描述。
     *
     * @return 失败摘要
     */
    public String summary() {
        return dataStreamName
                + "["
                + itemIndex
                + "]"
                + ": status="
                + status
                + ", type="
                + errorType
                + ", reason="
                + reason;
    }
}
