package io.github.stellspec.log;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Elaticsearch 写入进度日志器。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ElaticsearchWriteProgressLogger {

    private final StellspecLogProperties properties;

    private long totalMessages;

    private long totalDocuments;

    private long totalSuccessCount;

    private long totalFailureCount;

    private long totalSkippedOrMerged;

    private long windowMessages;

    private long windowDocuments;

    private long windowSuccessCount;

    private long windowFailureCount;

    private long windowSkippedOrMerged;

    private long lastInfoLogAt;

    private String lastTopic;

    private int lastPartition;

    private long lastOffset;

    private String lastDataStream;

    /**
     * 记录单条消息被过滤或合并抑制。
     *
     * @param message Stellflow 消息
     */
    public void recordSkipped(StellflowMessage message) {
        record(message, 1, 0, 0, 0, 1, null);
    }

    /**
     * 记录单条消息写入结果。
     *
     * @param message Stellflow 消息
     * @param documents 写入文档
     * @param result 写入结果
     */
    public void recordWrite(
            StellflowMessage message, List<RoutedLogDocument> documents, BulkWriteResult result) {
        int documentCount = documents == null ? 0 : documents.size();
        int successCount = result == null ? 0 : result.successCount();
        int failureCount = result == null ? 0 : result.failureCount();
        record(message, 1, documentCount, successCount, failureCount, 0, firstDataStream(documents));
    }

    /**
     * 记录批量消息写入结果。
     *
     * @param messages Stellflow 消息集合
     * @param documents 写入文档
     * @param result 写入结果
     */
    public void recordBatch(
            List<StellflowMessage> messages, List<RoutedLogDocument> documents, BulkWriteResult result) {
        int messageCount = messages == null ? 0 : messages.size();
        int documentCount = documents == null ? 0 : documents.size();
        int successCount = result == null ? 0 : result.successCount();
        int failureCount = result == null ? 0 : result.failureCount();
        long skippedOrMerged = Math.max(0, messageCount - documentCount);
        StellflowMessage lastMessage = messages == null || messages.isEmpty() ? null : messages.getLast();
        record(lastMessage, messageCount, documentCount, successCount, failureCount, skippedOrMerged, firstDataStream(documents));
    }

    private synchronized void record(
            StellflowMessage message,
            long messageCount,
            long documentCount,
            long successCount,
            long failureCount,
            long skippedOrMerged,
            String dataStream) {
        totalMessages += messageCount;
        totalDocuments += documentCount;
        totalSuccessCount += successCount;
        totalFailureCount += failureCount;
        totalSkippedOrMerged += skippedOrMerged;
        windowMessages += messageCount;
        windowDocuments += documentCount;
        windowSuccessCount += successCount;
        windowFailureCount += failureCount;
        windowSkippedOrMerged += skippedOrMerged;
        rememberLast(message, dataStream);
        maybeLog();
    }

    private void rememberLast(StellflowMessage message, String dataStream) {
        if (message != null) {
            lastTopic = message.topic();
            lastPartition = message.partition();
            lastOffset = message.offset();
        }
        if (dataStream != null) {
            lastDataStream = dataStream;
        }
    }

    private void maybeLog() {
        if (!properties.getObservability().isInfoSummaryEnabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        long intervalMillis = Math.max(1000L, properties.getObservability().getInfoSummaryIntervalMillis());
        if (lastInfoLogAt > 0 && now - lastInfoLogAt < intervalMillis) {
            return;
        }
        lastInfoLogAt = now;
        log.info(
                "Elaticsearch write progress messages={} documents={} successCount={} failureCount={} "
                        + "skippedOrMerged={} totalMessages={} totalDocuments={} totalSuccessCount={} "
                        + "totalFailureCount={} totalSkippedOrMerged={} lastTopic={} lastPartition={} "
                        + "lastOffset={} lastDataStream={}",
                windowMessages,
                windowDocuments,
                windowSuccessCount,
                windowFailureCount,
                windowSkippedOrMerged,
                totalMessages,
                totalDocuments,
                totalSuccessCount,
                totalFailureCount,
                totalSkippedOrMerged,
                lastTopic,
                lastPartition,
                lastOffset,
                lastDataStream);
        resetWindow();
    }

    private void resetWindow() {
        windowMessages = 0;
        windowDocuments = 0;
        windowSuccessCount = 0;
        windowFailureCount = 0;
        windowSkippedOrMerged = 0;
    }

    private String firstDataStream(List<RoutedLogDocument> documents) {
        if (documents == null || documents.isEmpty()) {
            return null;
        }
        return documents.getFirst().dataStreamName();
    }
}
