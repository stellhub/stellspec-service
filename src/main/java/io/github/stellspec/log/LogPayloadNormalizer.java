package io.github.stellspec.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.LogDocument;
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

/** Stellflow 日志消息归一化器。 */
@Component
@RequiredArgsConstructor
public class LogPayloadNormalizer {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    private final StellspecLogProperties properties;

    /**
     * 将 Stellflow 消息转换为可写入的日志文档。
     *
     * @param message Stellflow 消息
     * @return 日志文档
     */
    public LogDocument normalize(StellflowMessage message) {
        String payload = message.valueAsString();
        Map<String, Object> root = readObject(payload);
        ExtractedLog extractedLog = extractLog(root);
        Map<String, Object> logRecord = extractedLog.logRecord();
        Map<String, Object> attributes = attributesToMap(logRecord.get("attributes"));
        Map<String, Object> resourceAttributes = extractedLog.resourceAttributes();
        Instant eventTime = resolveEventTime(logRecord);

        return LogDocument.builder()
                .id(resolveDocumentId(message, payload))
                .indexName(properties.indexName(eventTime))
                .topic(message.topic())
                .partition(message.partition())
                .offset(message.offset())
                .eventTime(eventTime)
                .ingestedAt(Instant.now())
                .severityText(firstText(logRecord, "severityText", "severity", "level"))
                .severityNumber(firstInteger(logRecord, "severityNumber", "severity_number"))
                .traceId(firstText(logRecord, "traceId", "trace_id"))
                .spanId(firstText(logRecord, "spanId", "span_id"))
                .serviceName(resolveServiceName(resourceAttributes, logRecord))
                .body(resolveBody(logRecord))
                .messageKey(message.keyAsString())
                .attributes(attributes)
                .resourceAttributes(resourceAttributes)
                .rawPayload(properties.isIncludeRawPayload() ? payload : null)
                .build();
    }

    private Map<String, Object> readObject(String payload) {
        if (payload == null || payload.isBlank()) {
            return Map.of("body", "");
        }
        try {
            return objectMapper.readValue(payload, MAP_TYPE);
        } catch (JsonProcessingException exception) {
            return Map.of("body", payload);
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

    private Instant resolveEventTime(Map<String, Object> logRecord) {
        Object value = firstValue(logRecord, "timeUnixNano", "observedTimeUnixNano", "timestamp", "time", "@timestamp");
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
        if (text.isBlank()) {
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

    private String resolveBody(Map<String, Object> logRecord) {
        Object value = firstValue(logRecord, "body", "message", "log", "msg");
        Object unwrapped = unwrapOtelValue(value);
        if (unwrapped == null) {
            return "";
        }
        if (unwrapped instanceof String string) {
            return string;
        }
        try {
            return objectMapper.writeValueAsString(unwrapped);
        } catch (JsonProcessingException exception) {
            return String.valueOf(unwrapped);
        }
    }

    private String resolveServiceName(
            Map<String, Object> resourceAttributes, Map<String, Object> logRecord) {
        Object serviceName =
                firstValue(resourceAttributes, "service.name", "serviceName", "service");
        if (serviceName == null) {
            serviceName = firstValue(logRecord, "service.name", "serviceName", "service");
        }
        String resolved = serviceName == null ? null : String.valueOf(serviceName);
        return resolved == null || resolved.isBlank()
                ? properties.getFallbackServiceName()
                : resolved;
    }

    private String resolveDocumentId(StellflowMessage message, String payload) {
        if (message.offset() >= 0) {
            return message.topic() + "-" + message.partition() + "-" + message.offset();
        }
        return message.topic() + "-" + sha256(payload == null ? "" : payload);
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

    private Integer firstInteger(Map<String, Object> map, String... names) {
        Object value = firstValue(map, names);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Integer.parseInt(text);
        }
        return null;
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
}
