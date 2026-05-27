package io.github.stellspec.log.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EcsLogDocumentTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void sourceShouldSerializeNestedInstantAsIsoString() throws JsonProcessingException {
        Instant timestamp = Instant.parse("2026-05-27T01:02:03Z");
        Instant ingestedAt = Instant.parse("2026-05-27T01:02:04Z");
        EcsLogDocument document =
                EcsLogDocument.builder()
                        .id("doc-1")
                        .dataStreamName("logs-order-service-application-prod")
                        .timestamp(timestamp)
                        .message("order created")
                        .attributes(Map.of("processed_at", ingestedAt))
                        .stellspecIngest(new StellspecIngest(ingestedAt, true, false, null, null, null, null, null))
                        .build();

        Map<String, Object> source = document.toSource();

        assertThat(source).containsEntry("@timestamp", "2026-05-27T01:02:03Z");
        assertThat(source)
                .extractingByKey("stellspec")
                .asString()
                .contains("2026-05-27T01:02:04Z");
        objectMapper.writeValueAsString(source);
    }
}
