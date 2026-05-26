package io.github.stellspec.log.normalize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.EcsError;
import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLog;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.EcsService;
import io.github.stellspec.log.domain.EcsSpan;
import io.github.stellspec.log.domain.EcsTenant;
import io.github.stellspec.log.domain.EcsTrace;
import io.github.stellspec.log.domain.StellflowSource;
import io.github.stellspec.log.domain.StellspecIngest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** ECS 日志归一化器。 */
@Component
@RequiredArgsConstructor
public class EcsLogNormalizer {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    private final StellspecLogProperties properties;

    /**
     * 将 Stellflow 消息转换为 ECS 日志文档。
     *
     * @param message Stellflow 消息
     * @return ECS 日志文档
     */
    public EcsLogDocument normalize(StellflowMessage message) {
        String payload = message.valueAsString();
        Map<String, Object> root = readObject(payload);
        ExtractedLog extractedLog = extractLog(root);
        Map<String, Object> logRecord = extractedLog.logRecord();
        Map<String, Object> attributes = attributesToMap(logRecord.get("attributes"));
        Map<String, Object> resource = extractedLog.resourceAttributes();
        Instant timestamp = resolveEventTime(logRecord);
        String serviceName = resolveServiceName(resource, logRecord);
        String environment = firstText(resource, "service.environment", "deployment.environment.name");
        if (!hasText(environment)) {
            environment = properties.getDefaultEnvironment();
        }

        LargeMessage largeMessage = largeMessage(resolveMessage(logRecord));
        EcsError error = resolveError(logRecord, attributes);
        Map<String, Object> labels = labelsFrom(resource);
        labels.putAll(labelsFrom(logRecord));
        String rawPayload = rawPayload(payload);

        return EcsLogDocument.builder()
                .id(resolveDocumentId(message, payload))
                .timestamp(timestamp)
                .message(largeMessage.message())
                .log(new EcsLog(firstText(logRecord, "severityText", "severity", "level"), firstText(logRecord, "logger", "log.logger")))
                .service(new EcsService(serviceName, firstText(resource, "service.version"), environment))
                .event(new EcsEvent(properties.getDatasetDefault(), "event", List.of("application"), List.of("info"), null))
                .trace(new EcsTrace(firstText(logRecord, "traceId", "trace_id")))
                .span(new EcsSpan(firstText(logRecord, "spanId", "span_id")))
                .tenant(new EcsTenant(firstText(attributes, "tenant.id", "tenantId"), firstText(attributes, "tenant.name", "tenantName")))
                .error(error)
                .labels(labels)
                .attributes(attributes)
                .stellspecIngest(
                        new StellspecIngest(
                                Instant.now(),
                                true,
                                largeMessage.truncated(),
                                largeMessage.originalLength(),
                                largeMessage.preview(),
                                largeMessage.hash(),
                                largeMessage.truncated() ? "truncate-message" : "normal",
                                rawPayload))
                .stellflowSource(
                        new StellflowSource(
                                message.topic(),
                                message.partition(),
                                message.offset(),
                                message.keyAsString()))
                .build();
    }

    private Map<String, Object> readObject(String payload) {
        if (!hasText(payload)) {
            return Map.of("message", "");
        }
        try {
            return objectMapper.readValue(payload, MAP_TYPE);
        } catch (JsonProcessingException exception) {
            return Map.of("message", payload);
        }
    }

    private ExtractedLog extractLog(Map<String, Object> root) {
        List<Map<String, Object>> resourceLogs = mapList(root.get("resourceLogs"));
        if (resourceLogs.isEmpty()) {
            Map<String, Object> resource = mapValue(root.get("resource"));
            Map<String, Object> resourceAttributes = attributesToMap(resource.get("attributes"));
            if (resourceAttributes.isEmpty()) {
                resourceAttributes = attributesToMap(root.get("resource"));
            }
            return new ExtractedLog(root, resourceAttributes);
        }

        Map<String, Object> resourceLog = resourceLogs.getFirst();
        Map<String, Object> resourceAttributes =
                attributesToMap(mapValue(resourceLog.get("resource")).get("attributes"));
        for (Map<String, Object> scopeLog : mapList(resourceLog.get("scopeLogs"))) {
            List<Map<String, Object>> logRecords = mapList(scopeLog.get("logRecords"));
            if (!logRecords.isEmpty()) {
                return new ExtractedLog(logRecords.getFirst(), resourceAttributes);
            }
        }
        return new ExtractedLog(resourceLog, resourceAttributes);
    }

    private EcsError resolveError(Map<String, Object> logRecord, Map<String, Object> attributes) {
        Map<String, Object> exception = mapValue(logRecord.get("exception"));
        String type = firstText(attributes, "exception.type", "error.type");
        String message = firstText(attributes, "exception.message", "error.message");
        String stackTrace = firstText(attributes, "exception.stacktrace", "exception.stack_trace", "error.stack_trace");
        String code = firstText(attributes, "exception.code", "error.code");
        if (!hasText(type)) {
            type = firstText(exception, "type", "exception.type", "error.type");
        }
        if (!hasText(message)) {
            message = firstText(exception, "message", "exception.message", "error.message");
        }
        if (!hasText(stackTrace)) {
            stackTrace = firstText(exception, "stacktrace", "stack_trace", "exception.stacktrace", "error.stack_trace");
        }
        if (!hasText(code)) {
            code = firstText(exception, "code", "error.code");
        }
        if (!hasText(type) && !hasText(message) && !hasText(stackTrace) && !hasText(code)) {
            return null;
        }
        return new EcsError(type, message, stackTrace, code);
    }

    private Instant resolveEventTime(Map<String, Object> logRecord) {
        Object value =
                firstValue(logRecord, "timeUnixNano", "observedTimeUnixNano", "timestamp", "time", "@timestamp");
        Instant parsed = parseInstant(value);
        return parsed == null ? Instant.now() : parsed;
    }

    private Instant parseInstant(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return parseEpochNumber(number.longValue());
        }
        String text = String.valueOf(value);
        if (!hasText(text)) {
            return null;
        }
        if (text.chars().allMatch(Character::isDigit)) {
            return parseEpochNumber(Long.parseLong(text));
        }
        return Instant.parse(text);
    }

    private Instant parseEpochNumber(long value) {
        if (value > 9_999_999_999_999_999L) {
            return Instant.ofEpochSecond(0, value);
        }
        if (value > 9_999_999_999L) {
            return Instant.ofEpochMilli(value);
        }
        return Instant.ofEpochSecond(value);
    }

    private String resolveMessage(Map<String, Object> logRecord) {
        Object value = firstValue(logRecord, "body", "message", "log", "msg");
        Object unwrapped = unwrapOtelValue(value);
        if (unwrapped == null) {
            return "";
        }
        if (unwrapped instanceof String text) {
            return text;
        }
        try {
            return objectMapper.writeValueAsString(unwrapped);
        } catch (JsonProcessingException exception) {
            return String.valueOf(unwrapped);
        }
    }

    private String resolveServiceName(Map<String, Object> resource, Map<String, Object> logRecord) {
        Object serviceName = firstValue(resource, "service.name", "serviceName", "service");
        if (serviceName == null) {
            serviceName = firstValue(logRecord, "service.name", "serviceName", "service");
        }
        String resolved = serviceName == null ? null : String.valueOf(serviceName);
        return hasText(resolved) ? resolved : properties.getFallbackServiceName();
    }

    private LargeMessage largeMessage(String message) {
        if (message == null) {
            return new LargeMessage("", false, 0, null, null);
        }
        int originalLength = message.length();
        if (originalLength <= properties.getMaxMessageLength()) {
            return new LargeMessage(message, false, originalLength, null, null);
        }
        String preview = message.substring(0, properties.getMaxMessageLength());
        return new LargeMessage(preview, true, originalLength, preview, sha256(message));
    }

    private String rawPayload(String payload) {
        if (!properties.isIncludeRawPayload() || payload == null) {
            return null;
        }
        if (payload.length() > properties.getMaxRawPayloadLength()) {
            return null;
        }
        return payload;
    }

    private String resolveDocumentId(StellflowMessage message, String payload) {
        if (message.offset() >= 0) {
            return message.topic() + "-" + message.partition() + "-" + message.offset();
        }
        return message.topic() + "-" + sha256(payload == null ? "" : payload);
    }

    private Map<String, Object> labelsFrom(Map<String, Object> source) {
        Map<String, Object> labels = new LinkedHashMap<>();
        for (String key : List.of("region", "az", "cluster", "host.name", "host.ip")) {
            Object value = source.get(key);
            if (value != null) {
                labels.put(key, unwrapOtelValue(value));
            }
        }
        return labels;
    }

    private Map<String, Object> attributesToMap(Object value) {
        Object unwrapped = unwrapOtelValue(value);
        if (unwrapped instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(String.valueOf(key), unwrapOtelValue(item)));
            return result;
        }
        if (unwrapped instanceof List<?> list) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Object item : list) {
                Map<String, Object> attribute = mapValue(item);
                Object key = attribute.get("key");
                if (key != null) {
                    result.put(String.valueOf(key), unwrapOtelValue(attribute.get("value")));
                }
            }
            return result;
        }
        return Map.of();
    }

    private Object unwrapOtelValue(Object value) {
        if (!(value instanceof Map<?, ?> rawMap)) {
            return value;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        rawMap.forEach((key, item) -> map.put(String.valueOf(key), item));
        for (String key :
                List.of("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue")) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        if (map.containsKey("arrayValue")) {
            List<Object> values = new ArrayList<>();
            Object rawValues = mapValue(map.get("arrayValue")).get("values");
            for (Object item : listValue(rawValues)) {
                values.add(unwrapOtelValue(item));
            }
            return values;
        }
        if (map.containsKey("kvlistValue")) {
            return attributesToMap(mapValue(map.get("kvlistValue")).get("values"));
        }
        return map;
    }

    private Object firstValue(Map<String, Object> map, String... names) {
        for (String name : names) {
            Object value = map.get(name);
            if (value != null) {
                return unwrapOtelValue(value);
            }
        }
        return null;
    }

    private String firstText(Map<String, Object> map, String... names) {
        Object value = firstValue(map, names);
        return value == null ? null : String.valueOf(value);
    }

    private List<Map<String, Object>> mapList(Object value) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : listValue(value)) {
            result.add(mapValue(item));
        }
        return result;
    }

    private List<?> listValue(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        return List.of();
    }

    private Map<String, Object> mapValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(String.valueOf(key), item));
            return result;
        }
        return Map.of();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String sha256(String payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 digest is not available", exception);
        }
    }

    private record ExtractedLog(
            Map<String, Object> logRecord, Map<String, Object> resourceAttributes) {}

    private record LargeMessage(
            String message, boolean truncated, int originalLength, String preview, String hash) {}
}
