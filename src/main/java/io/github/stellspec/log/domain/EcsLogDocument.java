package io.github.stellspec.log.domain;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.With;

/** ECS 风格日志文档。 */
@Getter
@With
@Builder
public class EcsLogDocument {

    private final String id;

    private final String dataStreamName;

    private final Instant timestamp;

    private final String message;

    private final EcsLog log;

    private final EcsService service;

    private final EcsEvent event;

    private final EcsTrace trace;

    private final EcsSpan span;

    private final EcsTenant tenant;

    private final EcsError error;

    private final Map<String, Object> labels;

    private final Map<String, Object> attributes;

    private final StellspecIngest stellspecIngest;

    private final StellflowSource stellflowSource;

    /**
     * 转换为 Elaticsearch data stream 文档。
     *
     * @return 文档字段
     */
    public Map<String, Object> toSource() {
        Map<String, Object> source = new LinkedHashMap<>();
        put(source, "@timestamp", timestamp);
        put(source, "message", message);
        put(source, "log", logToMap(log));
        put(source, "service", serviceToMap(service));
        put(source, "event", eventToMap(event));
        put(source, "trace", traceToMap(trace));
        put(source, "span", spanToMap(span));
        put(source, "tenant", tenantToMap(tenant));
        put(source, "error", errorToMap(error));
        put(source, "labels", labels == null ? Map.of() : labels);
        put(source, "attributes", attributes == null ? Map.of() : attributes);
        put(source, "stellspec", Map.of("ingest", ingestToMap(stellspecIngest)));
        put(source, "stellflow", sourceToMap(stellflowSource));
        return ElaticsearchSourceValues.toSourceMap(source);
    }

    private Map<String, Object> logToMap(EcsLog value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "level", value.level());
        put(map, "logger", value.logger());
        return map;
    }

    private Map<String, Object> serviceToMap(EcsService value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "name", value.name());
        put(map, "version", value.version());
        put(map, "environment", value.environment());
        return map;
    }

    private Map<String, Object> eventToMap(EcsEvent value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "dataset", value.dataset());
        put(map, "kind", value.kind());
        put(map, "category", safeList(value.category()));
        put(map, "type", safeList(value.type()));
        put(map, "hash", value.hash());
        return map;
    }

    private Map<String, Object> tenantToMap(EcsTenant value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "id", value.id());
        put(map, "name", value.name());
        return map;
    }

    private Map<String, Object> traceToMap(EcsTrace value) {
        if (value == null || value.id() == null) {
            return null;
        }
        return Map.of("id", value.id());
    }

    private Map<String, Object> spanToMap(EcsSpan value) {
        if (value == null || value.id() == null) {
            return null;
        }
        return Map.of("id", value.id());
    }

    private Map<String, Object> errorToMap(EcsError value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "type", value.type());
        put(map, "message", value.message());
        put(map, "stack_trace", value.stackTrace());
        put(map, "code", value.code());
        return map;
    }

    private Map<String, Object> ingestToMap(StellspecIngest value) {
        if (value == null) {
            return Map.of();
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "ingested_at", value.ingestedAt());
        put(map, "sampled", value.sampled());
        put(map, "truncated", value.truncated());
        put(map, "original_length", value.originalLength());
        put(map, "message_preview", value.messagePreview());
        put(map, "message_hash", value.messageHash());
        put(map, "policy", value.policy());
        put(map, "raw_payload", value.rawPayload());
        return map;
    }

    private Map<String, Object> sourceToMap(StellflowSource value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> map = new LinkedHashMap<>();
        put(map, "topic", value.topic());
        put(map, "partition", value.partition());
        put(map, "offset", value.offset());
        put(map, "message_key", value.messageKey());
        return map;
    }

    private List<String> safeList(List<String> value) {
        return value == null ? List.of() : value;
    }

    private void put(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
