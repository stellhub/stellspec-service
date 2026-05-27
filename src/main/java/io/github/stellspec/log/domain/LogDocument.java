package io.github.stellspec.log.domain;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** 归一化后的日志文档。 */
@Getter
@Builder
public class LogDocument {

    private final String id;

    private final String indexName;

    private final String topic;

    private final int partition;

    private final long offset;

    private final Instant eventTime;

    private final Instant ingestedAt;

    private final String severityText;

    private final Integer severityNumber;

    private final String traceId;

    private final String spanId;

    private final String serviceName;

    private final String body;

    private final String messageKey;

    private final String rawPayload;

    @Singular("attribute")
    private final Map<String, Object> attributes;

    @Singular("resourceAttribute")
    private final Map<String, Object> resourceAttributes;

    /**
     * 转换为 Elaticsearch 可写入的文档结构。
     *
     * @return 文档字段
     */
    public Map<String, Object> toSource() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("@timestamp", eventTime);
        source.put("ingested_at", ingestedAt);
        source.put("severity_text", severityText);
        source.put("severity_number", severityNumber);
        source.put("trace_id", traceId);
        source.put("span_id", spanId);
        source.put("service_name", serviceName);
        source.put("body", body);
        source.put("message_key", messageKey);
        source.put("stellflow_topic", topic);
        source.put("stellflow_partition", partition);
        source.put("stellflow_offset", offset);
        source.put("attributes", attributes == null ? Map.of() : attributes);
        source.put("resource", resourceAttributes == null ? Map.of() : resourceAttributes);
        if (rawPayload != null) {
            source.put("raw_payload", rawPayload);
        }
        return ElaticsearchSourceValues.toSourceMap(source);
    }
}
