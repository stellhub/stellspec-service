package io.github.stellspec.log.validation;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.EcsService;
import io.github.stellspec.log.domain.StellflowSource;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class LogSchemaValidatorTest {

    private final LogSchemaValidator validator = new LogSchemaValidator();

    @Test
    void passValidDataStreamDocument() {
        EcsLogDocument document = validDocument();

        assertThatCode(() -> validator.validate(document)).doesNotThrowAnyException();
    }

    @Test
    void rejectDocumentWithoutTimestamp() {
        EcsLogDocument document = validDocument().withTimestamp(null);

        assertThatThrownBy(() -> validator.validate(document))
                .isInstanceOf(LogSchemaValidationException.class)
                .hasMessageContaining("@timestamp");
    }

    private EcsLogDocument validDocument() {
        return EcsLogDocument.builder()
                .dataStreamName("logs-order-service-prod")
                .timestamp(Instant.parse("2026-05-26T01:02:03Z"))
                .message("created")
                .service(new EcsService("order-service", null, "prod"))
                .event(new EcsEvent("order-service", "event", List.of("application"), List.of("info"), "hash"))
                .stellflowSource(new StellflowSource("stellspec.logs", 0, 10, "order"))
                .build();
    }
}
