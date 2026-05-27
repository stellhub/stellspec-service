package io.github.stellspec.log;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.IngestionResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.writer.BulkFailureHandler;
import io.github.stellspec.log.writer.BulkFailureHandlingResult;
import io.github.stellspec.log.writer.BulkLogBuffer;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** StellSpec 日志摄取服务。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StellspecLogIngestionService {

    private final LogProcessingPipeline pipeline;

    private final BulkLogBuffer bulkLogBuffer;

    private final BulkFailureHandler bulkFailureHandler;

    private final ElaticsearchWriteProgressLogger writeProgressLogger;

    /**
     * 归一化并写入 Stellflow 日志消息。
     *
     * @param message Stellflow 消息
     * @return 写入结果
     */
    public IngestionResult ingest(StellflowMessage message) {
        List<RoutedLogDocument> documents = pipeline.process(message);
        if (documents.isEmpty()) {
            log.debug(
                    "Skipped Stellflow log topic={} partition={} offset={} reason=filtered_or_merged",
                    message.topic(),
                    message.partition(),
                    message.offset());
            writeProgressLogger.recordSkipped(message);
            return new IngestionResult(
                    null,
                    null,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    Instant.now(),
                    "skipped");
        }
        BulkWriteResult result = bulkLogBuffer.writeNow(documents);
        BulkFailureHandlingResult handlingResult =
                bulkFailureHandler.handle(documents, result, List.of(message), "manual-ingest");
        writeProgressLogger.recordWrite(message, documents, result);
        RoutedLogDocument first = documents.isEmpty() ? null : documents.getFirst();
        String dataStreamName = first == null ? null : first.dataStreamName();
        String id = first == null ? null : first.document().getId();
        Instant eventTime = first == null ? Instant.now() : first.document().getTimestamp();
        log.debug(
                "Indexed Stellflow log topic={} partition={} offset={} dataStream={} documentCount={} successCount={} failureCount={}",
                message.topic(),
                message.partition(),
                message.offset(),
                dataStreamName,
                documents.size(),
                result.successCount(),
                result.failureCount());
        return new IngestionResult(
                id,
                dataStreamName,
                message.topic(),
                message.partition(),
                message.offset(),
                eventTime,
                status(result, handlingResult));
    }

    /**
     * 批量归一化并写入 Stellflow 日志消息。
     *
     * @param messages Stellflow 消息集合
     * @return Bulk 写入结果
     */
    public BulkWriteResult ingestBatch(List<StellflowMessage> messages) {
        List<RoutedLogDocument> documents =
                messages.stream().flatMap(message -> pipeline.process(message).stream()).toList();
        if (documents.isEmpty()) {
            BulkWriteResult result = BulkWriteResult.success(0);
            writeProgressLogger.recordBatch(messages, documents, result);
            return result;
        }
        BulkWriteResult result = bulkLogBuffer.writeNow(documents);
        bulkFailureHandler.handle(documents, result, messages, "batch-ingest");
        writeProgressLogger.recordBatch(messages, documents, result);
        return result;
    }

    private String status(BulkWriteResult result, BulkFailureHandlingResult handlingResult) {
        if (result.failureCount() == 0) {
            return "created";
        }
        if (handlingResult.retrySuccessCount() == result.failureCount()
                && handlingResult.deadLetterCount() == 0
                && handlingResult.unresolvedFailureCount() == 0) {
            return "created_after_retry";
        }
        if (handlingResult.deadLetterCount() > 0 && handlingResult.fullyHandled()) {
            return "dead_letter";
        }
        return "partial_failure";
    }
}
