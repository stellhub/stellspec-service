package io.github.stellspec.log.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogDocumentTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void sourceShouldSerializeInstantAsIsoString() throws JsonProcessingException {
        Instant eventTime = Instant.parse("2026-05-27T01:02:03Z");
        Instant ingestedAt = Instant.parse("2026-05-27T01:02:04Z");
        LogDocument document =
                LogDocument.builder()
                        .id("doc-1")
                        .indexName("stellspec-logs-2026.05.27")
                        .topic("stellspec.logs")
                        .partition(0)
                        .offset(1)
                        .eventTime(eventTime)
                        .ingestedAt(ingestedAt)
                        .severityText("INFO")
                        .serviceName("order-service")
                        .body("order created")
                        .attribute("nested", Map.of("seen_at", ingestedAt))
                        .resourceAttribute("events", List.of(Map.of("time", eventTime)))
                        .build();

        Map<String, Object> source = document.toSource();

        assertThat(source)
                .containsEntry("@timestamp", "2026-05-27T01:02:03Z")
                .containsEntry("ingested_at", "2026-05-27T01:02:04Z");
        objectMapper.writeValueAsString(source);
    }
}
