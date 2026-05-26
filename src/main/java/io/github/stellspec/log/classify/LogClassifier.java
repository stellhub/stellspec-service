package io.github.stellspec.log.classify;

import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.EcsLogDocument;
import java.util.Locale;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 日志分类器。 */
@Component
@RequiredArgsConstructor
public class LogClassifier {

    private final StellspecLogProperties properties;

    private final RetentionPolicyResolver retentionPolicyResolver;

    /**
     * 根据 ECS 字段计算日志分类。
     *
     * @param document ECS 日志文档
     * @return 分类结果
     */
    public LogClassificationResult classify(EcsLogDocument document) {
        LogCategory category = category(document);
        String serviceName = document.getService() == null ? null : document.getService().name();
        String baseDataset = sanitize(hasText(serviceName) ? serviceName : properties.getDatasetDefault());
        String dataset =
                switch (category) {
                    case ERROR -> baseDataset + "-" + properties.getClassification().getErrorDatasetSuffix();
                    case ACCESS -> baseDataset + "-" + properties.getClassification().getAccessDatasetSuffix();
                    case AUDIT -> properties.getClassification().getAuditDataset();
                    case DEBUG -> baseDataset + "-" + properties.getClassification().getDebugDatasetSuffix();
                    case AGGREGATE -> properties.getClassification().getAggregateDataset();
                    default -> baseDataset;
                };
        return new LogClassificationResult(
                category,
                dataset,
                namespace(document),
                retentionPolicyResolver.resolve(category));
    }

    private LogCategory category(EcsLogDocument document) {
        String level = document.getLog() == null ? null : document.getLog().level();
        Map<String, Object> attributes = document.getAttributes() == null ? Map.of() : document.getAttributes();
        if (hasText(value(attributes, "audit.action")) || hasText(value(attributes, "audit.type"))) {
            return LogCategory.AUDIT;
        }
        if (hasText(value(attributes, "http.request.method")) || hasText(value(attributes, "url.path"))) {
            return LogCategory.ACCESS;
        }
        if (document.getError() != null || "ERROR".equalsIgnoreCase(level) || "FATAL".equalsIgnoreCase(level)) {
            return LogCategory.ERROR;
        }
        if ("DEBUG".equalsIgnoreCase(level) || "TRACE".equalsIgnoreCase(level)) {
            return LogCategory.DEBUG;
        }
        return LogCategory.APPLICATION;
    }

    private String namespace(EcsLogDocument document) {
        if (document.getTenant() != null && hasText(document.getTenant().id())) {
            return sanitize(document.getTenant().id());
        }
        if (document.getService() != null && hasText(document.getService().environment())) {
            return sanitize(document.getService().environment());
        }
        return sanitize(properties.getNamespaceDefault());
    }

    private String value(Map<String, Object> values, String key) {
        Object value = values.get(key);
        return value == null ? null : String.valueOf(value);
    }

    private String sanitize(String value) {
        String sanitized = value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_-]+", "-");
        sanitized = sanitized.replaceAll("-+", "-").replaceAll("(^-|-$)", "");
        return sanitized.isBlank() ? properties.getDatasetDefault() : sanitized;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
