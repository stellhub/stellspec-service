package io.github.stellspec.log.filter;

import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.EcsLogDocument;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 日志过滤和脱敏链。 */
@Component
@RequiredArgsConstructor
public class LogFilterChain {

    private final StellspecLogProperties properties;

    private final Random random = new Random();

    /**
     * 判断日志是否应该写入。
     *
     * @param document ECS 日志文档
     * @return 是否保留
     */
    public boolean shouldKeep(EcsLogDocument document) {
        if (dropHealthCheckAccessLog(document)) {
            return false;
        }
        return keepDebugLog(document);
    }

    /**
     * 对日志动态字段执行脱敏。
     *
     * @param document ECS 日志文档
     * @return 脱敏后的日志文档
     */
    public EcsLogDocument redact(EcsLogDocument document) {
        return document.withAttributes(redactMap(document.getAttributes()));
    }

    private boolean dropHealthCheckAccessLog(EcsLogDocument document) {
        if (!properties.getFilter().isDropHealthCheckAccessLogs()) {
            return false;
        }
        Map<String, Object> attributes = document.getAttributes() == null ? Map.of() : document.getAttributes();
        Object path = attributes.get("url.path");
        if (path == null) {
            path = attributes.get("http.target");
        }
        String resolvedPath = path == null ? "" : String.valueOf(path);
        return properties.getFilter().getHealthCheckPaths().contains(resolvedPath);
    }

    private boolean keepDebugLog(EcsLogDocument document) {
        String level = document.getLog() == null ? null : document.getLog().level();
        if (!"DEBUG".equalsIgnoreCase(level) && !"TRACE".equalsIgnoreCase(level)) {
            return true;
        }
        double sampleRate = properties.getFilter().getDebugSampleRate();
        return sampleRate >= 1D || random.nextDouble() < Math.max(0D, sampleRate);
    }

    private Map<String, Object> redactMap(Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> redacted = new LinkedHashMap<>();
        source.forEach((key, value) -> redacted.put(key, redactValue(key, value)));
        return redacted;
    }

    private Object redactValue(String key, Object value) {
        if (key == null || value == null) {
            return value;
        }
        String normalizedKey = key.toLowerCase(Locale.ROOT);
        for (String sensitive : properties.getFilter().getSensitiveFieldNames()) {
            if (normalizedKey.contains(sensitive.toLowerCase(Locale.ROOT))) {
                return "sha256:" + sha256(String.valueOf(value));
            }
        }
        return value;
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 digest is not available", exception);
        }
    }
}
