package io.github.stellspec.log.domain;

/** Bulk item 失败分类。 */
public enum BulkFailureType {
    RETRYABLE,
    DEAD_LETTER;

    /**
     * 根据 Elaticsearch 返回状态判断失败分类。
     *
     * @param status HTTP 状态码
     * @param errorType 错误类型
     * @return 失败分类
     */
    public static BulkFailureType classify(Integer status, String errorType) {
        if (status == null) {
            return RETRYABLE;
        }
        if (status == 408
                || status == 429
                || status == 500
                || status == 502
                || status == 503
                || status == 504) {
            return RETRYABLE;
        }
        if (errorType != null
                && (errorType.contains("timeout")
                        || errorType.contains("rejected_execution")
                        || errorType.contains("circuit_breaking"))) {
            return RETRYABLE;
        }
        return DEAD_LETTER;
    }
}
