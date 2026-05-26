package io.github.stellspec.log.consumer;

import io.github.stellflux.stellflow.StellfluxStellflowProperties;
import io.github.stellflux.stellflow.consumer.StellfluxStellflowConsumerFactory;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellhub.stellflow.sdk.consumer.ConsumerRecord;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumer;
import io.github.stellhub.stellflow.sdk.consumer.StellflowConsumerOptions;
import io.github.stellspec.log.domain.StellflowSource;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.LogProcessingPipeline;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.writer.BulkFailureHandler;
import io.github.stellspec.log.writer.BulkFailureHandlingResult;
import io.github.stellspec.log.writer.BulkLogBuffer;
import io.github.stellspec.log.writer.DeadLetterLogWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** 独立 Stellflow 批量消费 worker。 */
@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class StellflowBulkConsumerWorker implements SmartLifecycle {

    private final StellspecLogProperties logProperties;

    private final StellfluxStellflowProperties stellflowProperties;

    private final ObjectProvider<StellfluxStellflowConsumerFactory> consumerFactoryProvider;

    private final LogProcessingPipeline pipeline;

    private final BulkLogBuffer bulkLogBuffer;

    private final BulkFailureHandler bulkFailureHandler;

    private final DeadLetterLogWriter deadLetterLogWriter;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Object consumerLock = new Object();

    private final Map<TopicPartitionKey, CompletableFuture<Void>> partitionChains = new ConcurrentHashMap<>();

    private final Map<TopicPartitionKey, Long> committedOffsets = new ConcurrentHashMap<>();

    private final AtomicInteger inFlightRecords = new AtomicInteger();

    private final AtomicLong pollCount = new AtomicLong();

    private final AtomicLong polledRecordCount = new AtomicLong();

    private final AtomicLong processedRecordCount = new AtomicLong();

    private final AtomicLong filteredRecordCount = new AtomicLong();

    private final AtomicLong committedOffsetCount = new AtomicLong();

    private final AtomicLong commitFailureCount = new AtomicLong();

    private final AtomicLong backpressurePauseCount = new AtomicLong();

    private volatile Instant lastPollAt;

    private volatile Instant lastCommitAt;

    private volatile Instant lastFailureAt;

    private volatile String lastError;

    private ExecutorService pollExecutor;

    private ExecutorService partitionExecutor;

    private StellflowConsumer consumer;

    /**
     * 启动独立批量消费 worker。
     */
    @Override
    public void start() {
        if (!logProperties.getBulkConsumer().isEnabled() || running.get()) {
            return;
        }
        StellfluxStellflowConsumerFactory factory = consumerFactoryProvider.getIfAvailable();
        List<String> topics = stellflowProperties.getConsumer().effectiveTopics();
        if (factory == null || topics.isEmpty()) {
            log.warn("Stellflow bulk consumer worker skipped because factory or topics are missing");
            return;
        }
        this.consumer = factory.createConsumer(buildConsumerOptions());
        this.pollExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "stellspec-stellflow-poll");
            thread.setDaemon(true);
            return thread;
        });
        this.partitionExecutor =
                Executors.newFixedThreadPool(
                        Math.max(1, logProperties.getBulkConsumer().getPartitionWorkerThreads()),
                        runnable -> {
                            Thread thread = new Thread(runnable, "stellspec-stellflow-partition-worker");
                            thread.setDaemon(true);
                            return thread;
                        });
        if (running.compareAndSet(false, true)) {
            pollExecutor.execute(() -> runLoop(topics));
        }
    }

    /**
     * 停止独立批量消费 worker。
     */
    @Override
    public void stop() {
        running.set(false);
        if (consumer != null) {
            consumer.close();
        }
        if (pollExecutor != null) {
            pollExecutor.shutdownNow();
        }
        if (partitionExecutor != null) {
            partitionExecutor.shutdown();
            try {
                if (!partitionExecutor.awaitTermination(timeoutMillis(), TimeUnit.MILLISECONDS)) {
                    partitionExecutor.shutdownNow();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                partitionExecutor.shutdownNow();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return logProperties.getBulkConsumer().isEnabled();
    }

    private void runLoop(List<String> topics) {
        try {
            synchronized (consumerLock) {
                consumer.subscribe(topics).get(timeoutMillis(), TimeUnit.MILLISECONDS);
            }
            log.info("Started Stellflow bulk consumer topics={}", topics);
            while (running.get()) {
                if (shouldPausePoll()) {
                    backpressurePauseCount.incrementAndGet();
                    sleep(logProperties.getBulkConsumer().getBackpressureSleepMillis());
                    continue;
                }
                List<ConsumerRecord> records;
                synchronized (consumerLock) {
                    records = consumer.pollSync(pollTimeout());
                }
                pollCount.incrementAndGet();
                lastPollAt = Instant.now();
                if (records == null || records.isEmpty()) {
                    continue;
                }
                polledRecordCount.addAndGet(records.size());
                dispatchByPartition(records);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (Throwable throwable) {
            lastFailureAt = Instant.now();
            lastError = throwable.getClass().getName() + ": " + throwable.getMessage();
            if (running.get()) {
                log.warn("Stellflow bulk consumer loop failed", throwable);
                sleepAfterFailure();
                if (running.get()) {
                    runLoop(topics);
                }
            }
        }
    }

    private boolean shouldPausePoll() {
        return inFlightRecords.get() >= logProperties.getBulkConsumer().getMaxUncommittedRecords();
    }

    private void dispatchByPartition(List<ConsumerRecord> records) {
        Map<TopicPartitionKey, List<ConsumerRecord>> grouped = new LinkedHashMap<>();
        for (ConsumerRecord record : records) {
            grouped.computeIfAbsent(TopicPartitionKey.from(record), ignored -> new ArrayList<>()).add(record);
        }
        for (Map.Entry<TopicPartitionKey, List<ConsumerRecord>> entry : grouped.entrySet()) {
            TopicPartitionKey key = entry.getKey();
            List<ConsumerRecord> partitionRecords = entry.getValue();
            inFlightRecords.addAndGet(partitionRecords.size());
            partitionChains.compute(
                    key,
                    (ignored, current) -> {
                        CompletableFuture<Void> base = current == null ? CompletableFuture.completedFuture(null) : current;
                        return base.exceptionally(throwable -> null)
                                .thenRunAsync(
                                        () -> handlePartitionRecords(key, partitionRecords),
                                        partitionExecutor);
                    });
        }
    }

    private void handlePartitionRecords(TopicPartitionKey key, List<ConsumerRecord> records) {
        try {
            ProcessedPartitionBatch batch = processRecords(records);
            BulkWriteResult result = bulkLogBuffer.writeNow(batch.documents());
            if (result.failureCount() == 0) {
                batch.markDocumentOffsetsHandled();
            } else {
                BulkFailureHandlingResult handlingResult =
                        bulkFailureHandler.handle(batch.documents(), result, batch.messages(), "bulk-write");
                if (canCommitAfterHandling(handlingResult)) {
                    batch.markDocumentOffsetsHandled();
                } else {
                    batch.markDocumentOffsetsUnresolved(handlingResult.unresolvedSources());
                    if (handlingResult.unresolvedSources().isEmpty()) {
                        batch.markDocumentOffsetsUnresolved();
                    }
                }
            }
            commitHandledPrefix(key, records, batch.handledOffsets(), "partition-batch");
            processedRecordCount.addAndGet(batch.handledOffsets().size());
        } catch (Throwable throwable) {
            lastFailureAt = Instant.now();
            lastError = throwable.getClass().getName() + ": " + throwable.getMessage();
            log.warn("Failed to process Stellflow partition batch topic={} partition={}", key.topic(), key.partition(), throwable);
        } finally {
            inFlightRecords.addAndGet(-records.size());
        }
    }

    private ProcessedPartitionBatch processRecords(List<ConsumerRecord> records) {
        ProcessedPartitionBatch batch = new ProcessedPartitionBatch();
        for (ConsumerRecord record : records) {
            StellflowMessage message = StellflowMessage.fromConsumerRecord(record);
            batch.addMessage(message);
            try {
                List<RoutedLogDocument> documents = pipeline.process(message);
                if (documents.isEmpty()) {
                    batch.markHandled(record.offset());
                    filteredRecordCount.incrementAndGet();
                    continue;
                }
                batch.addDocuments(record.offset(), documents);
            } catch (Throwable throwable) {
                BulkWriteResult deadLetterResult =
                        deadLetterLogWriter.write(
                                List.of(message),
                                "process",
                                throwable.getClass().getName() + ": " + throwable.getMessage());
                if (deadLetterResult.failureCount() == 0
                        && logProperties.getBulkConsumer().isCommitAfterDeadLetter()) {
                    batch.markHandled(record.offset());
                } else {
                    batch.markUnresolved(record.offset());
                }
            }
        }
        return batch;
    }

    private boolean canCommitAfterHandling(BulkFailureHandlingResult handlingResult) {
        if (!handlingResult.fullyHandled()) {
            return false;
        }
        return handlingResult.deadLetterCount() == 0
                || logProperties.getBulkConsumer().isCommitAfterDeadLetter();
    }

    private void commitHandledPrefix(
            TopicPartitionKey key, List<ConsumerRecord> records, Set<Long> handledOffsets, String reason) {
        List<ConsumerRecord> sortedRecords = records.stream()
                .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                .toList();
        Long lastHandledOffset = null;
        for (ConsumerRecord record : sortedRecords) {
            if (!handledOffsets.contains(record.offset())) {
                break;
            }
            lastHandledOffset = record.offset();
        }
        if (lastHandledOffset == null) {
            return;
        }
        long nextOffset = lastHandledOffset + 1;
        try {
            synchronized (consumerLock) {
                consumer.commitOffset(
                                groupId(),
                                key.topic(),
                                key.partition(),
                                nextOffset,
                                stellflowProperties.getConsumer().getOffsetCommitMetadata())
                        .get(timeoutMillis(), TimeUnit.MILLISECONDS);
            }
            committedOffsetCount.incrementAndGet();
            committedOffsets.put(key, nextOffset);
            lastCommitAt = Instant.now();
            log.debug(
                    "Committed Stellflow offset groupId={} topic={} partition={} nextOffset={} reason={}",
                    groupId(),
                    key.topic(),
                    key.partition(),
                    nextOffset,
                    reason);
        } catch (Exception exception) {
            commitFailureCount.incrementAndGet();
            throw new IllegalStateException(
                    "Failed to commit Stellflow offset topic="
                            + key.topic()
                            + ", partition="
                            + key.partition()
                            + ", nextOffset="
                            + nextOffset,
                    exception);
        }
    }

    private StellflowConsumerOptions buildConsumerOptions() {
        StellfluxStellflowProperties.ConsumerProperties consumerProperties =
                stellflowProperties.getConsumer();
        return new StellflowConsumerOptions(
                groupId(),
                consumerProperties.getMemberId(),
                consumerProperties.getSessionTimeoutMs(),
                consumerProperties.getHeartbeatInterval(),
                consumerProperties.getFetchMaxBytes(),
                consumerProperties.getOffsetCommitMetadata());
    }

    private String groupId() {
        String configured = stellflowProperties.getConsumer().getGroupId();
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException("Stellflow consumer groupId is required");
        }
        return configured;
    }

    private Duration pollTimeout() {
        return Duration.ofMillis(logProperties.getBulkConsumer().getPollTimeoutMillis());
    }

    private long timeoutMillis() {
        return Math.max(1000L, stellflowProperties.getRequestTimeout().toMillis());
    }

    private void sleepAfterFailure() {
        sleep(logProperties.getBulkConsumer().getFailureBackoffMillis());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(Math.max(1L, millis));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    /** Topic partition key。 */
    public record TopicPartitionKey(String topic, int partition) {

        static TopicPartitionKey from(ConsumerRecord record) {
            return new TopicPartitionKey(record.topic(), record.partition());
        }
    }

    private static final class ProcessedPartitionBatch {

        private final List<StellflowMessage> messages = new ArrayList<>();

        private final List<RoutedLogDocument> documents = new ArrayList<>();

        private final Map<Long, List<RoutedLogDocument>> documentsByOffset = new LinkedHashMap<>();

        private final Set<Long> handledOffsets = new HashSet<>();

        private final Set<Long> unresolvedOffsets = new HashSet<>();

        private void addMessage(StellflowMessage message) {
            messages.add(message);
        }

        private void addDocuments(long offset, List<RoutedLogDocument> routedDocuments) {
            documents.addAll(routedDocuments);
            documentsByOffset.put(offset, routedDocuments);
        }

        private void markHandled(long offset) {
            handledOffsets.add(offset);
            unresolvedOffsets.remove(offset);
        }

        private void markUnresolved(long offset) {
            unresolvedOffsets.add(offset);
        }

        private void markDocumentOffsetsHandled() {
            documentsByOffset.keySet().forEach(this::markHandled);
        }

        private void markDocumentOffsetsUnresolved() {
            documentsByOffset.keySet().forEach(this::markUnresolved);
        }

        private void markDocumentOffsetsUnresolved(List<StellflowSource> sources) {
            Set<Long> unresolvedSourceOffsets = new HashSet<>();
            for (StellflowSource source : sources) {
                unresolvedSourceOffsets.add(source.offset());
            }
            documentsByOffset.keySet().forEach(offset -> {
                if (unresolvedSourceOffsets.contains(offset)) {
                    markUnresolved(offset);
                } else {
                    markHandled(offset);
                }
            });
        }

        private List<StellflowMessage> messages() {
            return messages;
        }

        private List<RoutedLogDocument> documents() {
            return documents;
        }

        private Set<Long> handledOffsets() {
            return handledOffsets;
        }
    }
}
