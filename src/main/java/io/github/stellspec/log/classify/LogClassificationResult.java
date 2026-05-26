package io.github.stellspec.log.classify;

/** 日志分类结果。 */
public record LogClassificationResult(
        LogCategory category, String dataset, String namespace, RetentionPolicy retentionPolicy) {

    public LogClassificationResult(LogCategory category, String dataset, String namespace) {
        this(category, dataset, namespace, RetentionPolicy.DEFAULT);
    }
}
