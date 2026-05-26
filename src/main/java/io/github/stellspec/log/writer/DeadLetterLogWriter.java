package io.github.stellspec.log.writer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.EcsService;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.domain.StellflowSource;
import io.github.stellspec.log.domain.StellspecIngest;
import io.github.stellflux.stellflow.message.StellflowMessage;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Dead letter data stream 写入器。 */
@Component
@RequiredArgsConstructor
public class DeadLetterLogWriter {

    private final ElasticsearchClient elasticsearchClient;

    private final StellspecLogProperties properties;

    /**
     * 将处理失败的 Stellflow 消息写入 dead letter data stream。
     *
     * @param messages 失败消息
     * @param stage 失败阶段
     * @param reason 失败原因
     * @return 写入结果
     */
    public BulkWriteResult write(List<StellflowMessage> messages, String stage, String reason) {
        if (messages == null || messages.isEmpty()) {
            return BulkWriteResult.success(0);
        }
        List<DeadLetterEntry> entries =
                messages.stream()
                        .map(message -> new DeadLetterEntry(message, stage, reason, null, null, null, null))
                        .toList();
        return writeEntries(entries);
    }

    /**
     * 将 item 级失败写入 dead letter data stream。
     *
     * @param entries dead letter 条目
     * @return 写入结果
     */
    public BulkWriteResult writeEntries(List<DeadLetterEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return BulkWriteResult.success(0);
        }
        List<RoutedLogDocument> documents = entries.stream().map(this::toDeadLetter).toList();
        try {
            BulkResponse response =
                    elasticsearchClient.bulk(
                            builder -> {
                                for (RoutedLogDocument document : documents) {
                                    builder.operations(
                                            operation ->
                                                    operation.create(
                                                            create ->
                                                                    create.index(document.dataStreamName())
                                                                            .document(document.document().toSource())));
                                }
                                return builder;
                            });
            return toResult(response, documents.size());
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write logs to dead letter data stream", exception);
        }
    }

    private BulkWriteResult toResult(BulkResponse response, int requestedCount) {
        List<String> failures =
                response.items().stream()
                        .filter(item -> item.error() != null)
                        .map(this::failure)
                        .toList();
        return new BulkWriteResult(requestedCount - failures.size(), failures.size(), failures);
    }

    private String failure(BulkResponseItem item) {
        return item.index() + ": " + item.error().reason();
    }

    private RoutedLogDocument toDeadLetter(DeadLetterEntry entry) {
        StellflowMessage message = entry.message();
        String dataStream = properties.getBulkConsumer().getDeadLetterDataStream();
        EcsLogDocument document =
                EcsLogDocument.builder()
                        .id(
                                "deadletter-"
                                        + message.topic()
                                        + "-"
                                        + message.partition()
                                        + "-"
                                        + message.offset())
                        .dataStreamName(dataStream)
                        .timestamp(Instant.now())
                        .message("StellSpec log ingestion failed")
                        .service(new EcsService("stellspec-service", null, properties.getDefaultEnvironment()))
                        .event(
                                new EcsEvent(
                                        "stellspec-deadletter",
                                        "event",
                                        List.of("application"),
                                        List.of("error"),
                                        null))
                        .attributes(attributes(message, entry))
                        .stellspecIngest(
                                new StellspecIngest(
                                        Instant.now(), true, false, null, null, null, "dead-letter", null))
                        .stellflowSource(
                                new StellflowSource(
                                        message.topic(),
                                        message.partition(),
                                        message.offset(),
                                        message.keyAsString()))
                        .build();
        return new RoutedLogDocument(dataStream, document);
    }

    private Map<String, Object> attributes(StellflowMessage message, DeadLetterEntry entry) {
        LinkedHashMap<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("stellspec.deadletter.stage", entry.stage());
        attributes.put("stellspec.deadletter.reason", entry.reason());
        attributes.put("stellspec.deadletter.payload", message.valueAsString() == null ? "" : message.valueAsString());
        put(attributes, "stellspec.deadletter.failed_data_stream", entry.failedDataStream());
        put(attributes, "stellspec.deadletter.failed_document_id", entry.failedDocumentId());
        put(attributes, "stellspec.deadletter.status", entry.status());
        put(attributes, "stellspec.deadletter.error_type", entry.errorType());
        return attributes;
    }

    private void put(Map<String, Object> attributes, String key, Object value) {
        if (value != null) {
            attributes.put(key, value);
        }
    }
}
