package io.github.stellspec.log.writer;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.BulkFailureType;
import io.github.stellspec.log.domain.BulkItemFailure;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.domain.StellflowSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Bulk item 级失败处理器。 */
@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class BulkFailureHandler {

    private final StellspecLogProperties properties;

    private final BulkLogWriter bulkLogWriter;

    private final DeadLetterLogWriter deadLetterLogWriter;

    private final AtomicLong retryAttemptCount = new AtomicLong();

    private final AtomicLong retryDocumentCount = new AtomicLong();

    private final AtomicLong retrySuccessCount = new AtomicLong();

    private final AtomicLong deadLetterCount = new AtomicLong();

    private final AtomicLong deadLetterFailureCount = new AtomicLong();

    private final AtomicLong unresolvedFailureCount = new AtomicLong();

    private volatile Instant lastFailureAt;

    private volatile String lastFailureSummary;

    /**
     * 处理 Bulk 写入失败。
     *
     * @param documents 原始写入文档
     * @param result 原始 Bulk 写入结果
     * @param sourceMessages 原始 Stellflow 消息
     * @param stage 失败阶段
     * @return 失败处理结果
     */
    public BulkFailureHandlingResult handle(
            List<RoutedLogDocument> documents,
            BulkWriteResult result,
            List<StellflowMessage> sourceMessages,
            String stage) {
        if (result == null || result.failureCount() == 0) {
            return BulkFailureHandlingResult.empty();
        }
        lastFailureAt = Instant.now();
        lastFailureSummary = String.join("; ", result.failures());

        List<RoutedLogDocument> safeDocuments = documents == null ? List.of() : documents;
        List<BulkItemFailure> failures = completeFailures(result, safeDocuments);
        List<FailedDocument> retryable = failedDocuments(safeDocuments, failures, BulkFailureType.RETRYABLE);
        List<FailedDocument> deadLetter = failedDocuments(safeDocuments, failures, BulkFailureType.DEAD_LETTER);

        RetryOutcome retryOutcome = retry(retryable);
        deadLetter.addAll(retryOutcome.remainingFailures());

        BulkWriteResult deadLetterResult =
                writeDeadLetters(deadLetter, sourceMessages, stage);
        List<StellflowSource> unresolvedSources = unresolvedSources(deadLetter, sourceMessages);
        int unresolved = unresolvedSources.size();
        if (deadLetterResult.failureCount() > 0) {
            deadLetterFailureCount.addAndGet(deadLetterResult.failureCount());
        }
        if (unresolved > 0) {
            unresolvedFailureCount.addAndGet(unresolved);
        }

        return new BulkFailureHandlingResult(
                result.failureCount(),
                retryOutcome.retriedCount(),
                retryOutcome.successCount(),
                deadLetter.size() - unresolved,
                deadLetterResult.failureCount(),
                unresolved,
                unresolvedSources);
    }

    private List<BulkItemFailure> completeFailures(
            BulkWriteResult result, List<RoutedLogDocument> documents) {
        if (!result.itemFailures().isEmpty()) {
            return result.itemFailures();
        }
        List<BulkItemFailure> failures = new ArrayList<>();
        for (int index = 0; index < documents.size(); index++) {
            RoutedLogDocument document = documents.get(index);
            failures.add(
                    new BulkItemFailure(
                            index,
                            document.dataStreamName(),
                            document.document().getId(),
                            null,
                            "unknown",
                            result.failures().isEmpty() ? "bulk item failed" : result.failures().getFirst(),
                            BulkFailureType.DEAD_LETTER));
        }
        return failures;
    }

    private List<FailedDocument> failedDocuments(
            List<RoutedLogDocument> documents,
            List<BulkItemFailure> failures,
            BulkFailureType type) {
        List<FailedDocument> failedDocuments = new ArrayList<>();
        for (BulkItemFailure failure : failures) {
            if (failure.failureType() != type || failure.itemIndex() < 0 || failure.itemIndex() >= documents.size()) {
                continue;
            }
            failedDocuments.add(new FailedDocument(documents.get(failure.itemIndex()), failure));
        }
        return failedDocuments;
    }

    private RetryOutcome retry(List<FailedDocument> retryableFailures) {
        if (retryableFailures.isEmpty() || properties.getBulk().getMaxRetries() <= 0) {
            return new RetryOutcome(0, 0, retryableFailures);
        }
        List<FailedDocument> remaining = new ArrayList<>(retryableFailures);
        List<FailedDocument> deadLetterAfterRetry = new ArrayList<>();
        int retried = 0;
        int succeeded = 0;
        for (int attempt = 1; attempt <= properties.getBulk().getMaxRetries() && !remaining.isEmpty(); attempt++) {
            sleepBeforeRetry(attempt);
            List<RoutedLogDocument> retryDocuments =
                    remaining.stream().map(FailedDocument::document).toList();
            retried += retryDocuments.size();
            retryAttemptCount.incrementAndGet();
            retryDocumentCount.addAndGet(retryDocuments.size());
            BulkWriteResult retryResult = bulkLogWriter.write(retryDocuments);
            if (retryResult.failureCount() == 0) {
                succeeded += retryDocuments.size();
                retrySuccessCount.addAndGet(retryDocuments.size());
                remaining = new ArrayList<>();
                break;
            }
            int attemptSucceeded = retryDocuments.size() - retryResult.failureCount();
            succeeded += attemptSucceeded;
            retrySuccessCount.addAndGet(attemptSucceeded);
            RetrySplit retrySplit = retryRemaining(retryDocuments, retryResult);
            remaining = retrySplit.retryableFailures();
            deadLetterAfterRetry.addAll(retrySplit.deadLetterFailures());
        }
        remaining.addAll(deadLetterAfterRetry);
        return new RetryOutcome(retried, succeeded, remaining);
    }

    private RetrySplit retryRemaining(
            List<RoutedLogDocument> retryDocuments, BulkWriteResult retryResult) {
        List<BulkItemFailure> retryFailures = completeFailures(retryResult, retryDocuments);
        List<FailedDocument> retryableFailures = new ArrayList<>();
        List<FailedDocument> deadLetterFailures = new ArrayList<>();
        for (BulkItemFailure failure : retryFailures) {
            if (failure.itemIndex() < 0 || failure.itemIndex() >= retryDocuments.size()) {
                continue;
            }
            FailedDocument failedDocument = new FailedDocument(retryDocuments.get(failure.itemIndex()), failure);
            if (failure.retryable()) {
                retryableFailures.add(failedDocument);
            } else {
                deadLetterFailures.add(failedDocument);
            }
        }
        return new RetrySplit(retryableFailures, deadLetterFailures);
    }

    private BulkWriteResult writeDeadLetters(
            List<FailedDocument> failedDocuments,
            List<StellflowMessage> sourceMessages,
            String stage) {
        List<DeadLetterEntry> entries = deadLetterEntries(failedDocuments, sourceMessages, stage);
        if (entries.isEmpty()) {
            return BulkWriteResult.success(0);
        }
        deadLetterCount.addAndGet(entries.size());
        return deadLetterLogWriter.writeEntries(entries);
    }

    private List<DeadLetterEntry> deadLetterEntries(
            List<FailedDocument> failedDocuments,
            List<StellflowMessage> sourceMessages,
            String stage) {
        Map<String, StellflowMessage> messagesBySource = messagesBySource(sourceMessages);
        List<DeadLetterEntry> entries = new ArrayList<>();
        for (FailedDocument failedDocument : failedDocuments) {
            StellflowSource source = failedDocument.document().document().getStellflowSource();
            StellflowMessage message = source == null ? null : messagesBySource.get(sourceKey(source));
            if (message == null) {
                continue;
            }
            BulkItemFailure failure = failedDocument.failure();
            entries.add(
                    new DeadLetterEntry(
                            message,
                            stage,
                            failure.summary(),
                            failure.dataStreamName(),
                            failure.documentId(),
                            failure.status(),
                            failure.errorType()));
        }
        return entries;
    }

    private Map<String, StellflowMessage> messagesBySource(List<StellflowMessage> messages) {
        Map<String, StellflowMessage> result = new LinkedHashMap<>();
        if (messages == null) {
            return result;
        }
        for (StellflowMessage message : messages) {
            result.put(sourceKey(message.topic(), message.partition(), message.offset()), message);
        }
        return result;
    }

    private List<StellflowSource> unresolvedSources(
            List<FailedDocument> failedDocuments, List<StellflowMessage> sourceMessages) {
        Map<String, StellflowMessage> messagesBySource = messagesBySource(sourceMessages);
        List<StellflowSource> unresolved = new ArrayList<>();
        for (FailedDocument failedDocument : failedDocuments) {
            StellflowSource source = failedDocument.document().document().getStellflowSource();
            if (source == null || !messagesBySource.containsKey(sourceKey(source))) {
                if (source != null) {
                    unresolved.add(source);
                }
            }
        }
        return unresolved;
    }

    private String sourceKey(StellflowSource source) {
        return sourceKey(source.topic(), source.partition(), source.offset());
    }

    private String sourceKey(String topic, int partition, long offset) {
        return topic + ":" + partition + ":" + offset;
    }

    private void sleepBeforeRetry(int attempt) {
        if (attempt <= 1 || properties.getBulk().getRetryBackoffMillis() <= 0) {
            return;
        }
        try {
            Thread.sleep(properties.getBulk().getRetryBackoffMillis());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private record FailedDocument(RoutedLogDocument document, BulkItemFailure failure) {}

    private record RetryOutcome(int retriedCount, int successCount, List<FailedDocument> remainingFailures) {}

    private record RetrySplit(List<FailedDocument> retryableFailures, List<FailedDocument> deadLetterFailures) {}
}
