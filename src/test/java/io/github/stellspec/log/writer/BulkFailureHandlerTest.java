package io.github.stellspec.log.writer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.BulkFailureType;
import io.github.stellspec.log.domain.BulkItemFailure;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.domain.StellflowSource;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class BulkFailureHandlerTest {

    private final StellspecLogProperties properties = new StellspecLogProperties();

    @Test
    void retryRetryableBulkItemFailureBeforeDeadLetter() {
        properties.getBulk().setRetryBackoffMillis(0);
        BulkLogWriter retryWriter = documents -> BulkWriteResult.success(documents.size());
        DeadLetterLogWriter deadLetterLogWriter = mock(DeadLetterLogWriter.class);
        BulkFailureHandler handler = new BulkFailureHandler(properties, retryWriter, deadLetterLogWriter);
        RoutedLogDocument document = routedDocument(message());
        BulkWriteResult result =
                new BulkWriteResult(
                        0,
                        1,
                        List.of("too many requests"),
                        List.of(
                                new BulkItemFailure(
                                        0,
                                        document.dataStreamName(),
                                        document.document().getId(),
                                        429,
                                        "es_rejected_execution_exception",
                                        "too many requests",
                                        BulkFailureType.RETRYABLE)));

        BulkFailureHandlingResult handlingResult =
                handler.handle(List.of(document), result, List.of(message()), "bulk-write");

        assertThat(handlingResult.fullyHandled()).isTrue();
        assertThat(handlingResult.retriedCount()).isEqualTo(1);
        assertThat(handlingResult.retrySuccessCount()).isEqualTo(1);
        assertThat(handlingResult.deadLetterCount()).isZero();
        assertThat(handler.getRetryAttemptCount()).hasValue(1);
        verify(deadLetterLogWriter, never()).writeEntries(anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendNonRetryableBulkItemFailureToDeadLetter() {
        BulkLogWriter retryWriter = documents -> BulkWriteResult.success(documents.size());
        DeadLetterLogWriter deadLetterLogWriter = mock(DeadLetterLogWriter.class);
        when(deadLetterLogWriter.writeEntries(anyList())).thenReturn(BulkWriteResult.success(1));
        BulkFailureHandler handler = new BulkFailureHandler(properties, retryWriter, deadLetterLogWriter);
        StellflowMessage message = message();
        RoutedLogDocument document = routedDocument(message);
        BulkWriteResult result =
                new BulkWriteResult(
                        0,
                        1,
                        List.of("mapper parsing exception"),
                        List.of(
                                new BulkItemFailure(
                                        0,
                                        document.dataStreamName(),
                                        document.document().getId(),
                                        400,
                                        "mapper_parsing_exception",
                                        "failed to parse field",
                                        BulkFailureType.DEAD_LETTER)));

        BulkFailureHandlingResult handlingResult =
                handler.handle(List.of(document), result, List.of(message), "bulk-write");

        assertThat(handlingResult.fullyHandled()).isTrue();
        assertThat(handlingResult.retriedCount()).isZero();
        assertThat(handlingResult.deadLetterCount()).isEqualTo(1);
        assertThat(handler.getDeadLetterCount()).hasValue(1);
        ArgumentCaptor<List<DeadLetterEntry>> captor = ArgumentCaptor.forClass(List.class);
        verify(deadLetterLogWriter).writeEntries(captor.capture());
        DeadLetterEntry entry = captor.getValue().getFirst();
        assertThat(entry.failedDataStream()).isEqualTo("logs-test-service-prod");
        assertThat(entry.status()).isEqualTo(400);
        assertThat(entry.errorType()).isEqualTo("mapper_parsing_exception");
    }

    private StellflowMessage message() {
        return StellflowMessage.ofString("stellspec.logs", "order", "{\"message\":\"created\"}");
    }

    private RoutedLogDocument routedDocument(StellflowMessage message) {
        EcsLogDocument document =
                EcsLogDocument.builder()
                        .id("doc-1")
                        .dataStreamName("logs-test-service-prod")
                        .timestamp(Instant.parse("2026-05-26T01:02:03Z"))
                        .message("created")
                        .stellflowSource(
                                new StellflowSource(
                                        message.topic(),
                                        message.partition(),
                                        message.offset(),
                                        message.keyAsString()))
                        .build();
        return new RoutedLogDocument("logs-test-service-prod", document);
    }
}
