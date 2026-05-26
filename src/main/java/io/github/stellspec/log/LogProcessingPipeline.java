package io.github.stellspec.log;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.log.classify.DataStreamNameResolver;
import io.github.stellspec.log.classify.LogCategory;
import io.github.stellspec.log.classify.LogClassificationResult;
import io.github.stellspec.log.classify.LogClassifier;
import io.github.stellspec.log.classify.RetentionPolicy;
import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.filter.LogFilterChain;
import io.github.stellspec.log.merge.LogFingerprintGenerator;
import io.github.stellspec.log.merge.LogMergeWindowAggregator;
import io.github.stellspec.log.normalize.EcsLogNormalizer;
import io.github.stellspec.log.validation.LogSchemaValidator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 日志处理流水线。 */
@Component
@RequiredArgsConstructor
public class LogProcessingPipeline {

    private final EcsLogNormalizer normalizer;

    private final LogFilterChain filterChain;

    private final LogFingerprintGenerator fingerprintGenerator;

    private final LogMergeWindowAggregator mergeWindowAggregator;

    private final LogClassifier classifier;

    private final DataStreamNameResolver dataStreamNameResolver;

    private final LogSchemaValidator schemaValidator;

    /**
     * 将 Stellflow 消息处理为可写入 data stream 的文档。
     *
     * @param message Stellflow 消息
     * @return 已路由文档集合
     */
    public List<RoutedLogDocument> process(StellflowMessage message) {
        EcsLogDocument normalized = normalizer.normalize(message);
        if (!filterChain.shouldKeep(normalized)) {
            return List.of();
        }
        EcsLogDocument redacted = filterChain.redact(normalized);
        LogClassificationResult classification = classifier.classify(redacted);
        String dataStreamName = dataStreamNameResolver.resolve(classification);
        String eventHash = fingerprintGenerator.generate(redacted);
        EcsEvent event = event(redacted, classification, eventHash);
        EcsLogDocument routed = redacted.withEvent(event).withDataStreamName(dataStreamName);
        return mergeWindowAggregator.aggregate(routed).stream()
                .map(this::routeMergedDocument)
                .peek(routedDocument -> schemaValidator.validate(routedDocument.document()))
                .toList();
    }

    private RoutedLogDocument routeMergedDocument(EcsLogDocument document) {
        if (document.getEvent() != null && "metric".equals(document.getEvent().kind())) {
            LogClassificationResult aggregateClassification =
                    new LogClassificationResult(
                            LogCategory.AGGREGATE,
                            "stellspec-aggregate",
                            classifier.classify(document).namespace(),
                            RetentionPolicy.AGGREGATE);
            String aggregateDataStream = dataStreamNameResolver.resolve(aggregateClassification);
            EcsEvent aggregateEvent =
                    new EcsEvent(
                            aggregateClassification.dataset(),
                            document.getEvent().kind(),
                            document.getEvent().category(),
                            document.getEvent().type(),
                            document.getEvent().hash());
            EcsLogDocument aggregateDocument =
                    document.withDataStreamName(aggregateDataStream).withEvent(aggregateEvent);
            return new RoutedLogDocument(aggregateDataStream, aggregateDocument);
        }
        return new RoutedLogDocument(document.getDataStreamName(), document);
    }

    private EcsEvent event(
            EcsLogDocument document, LogClassificationResult classification, String eventHash) {
        EcsEvent current = document.getEvent();
        String kind = current == null ? "event" : current.kind();
        return new EcsEvent(
                classification.dataset(),
                kind == null ? "event" : kind,
                current == null ? List.of("application") : current.category(),
                List.of(classification.category().name().toLowerCase()),
                eventHash);
    }
}
