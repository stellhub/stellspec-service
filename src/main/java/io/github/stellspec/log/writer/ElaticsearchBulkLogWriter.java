package io.github.stellspec.log.writer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import io.github.stellspec.log.domain.BulkFailureType;
import io.github.stellspec.log.domain.BulkItemFailure;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Elaticsearch Bulk API 日志写入器。 */
@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class ElaticsearchBulkLogWriter implements BulkLogWriter {

    private final ElasticsearchClient elasticsearchClient;

    private final AtomicLong flushCount = new AtomicLong();

    private final AtomicLong successCount = new AtomicLong();

    private final AtomicLong failureCount = new AtomicLong();

    /**
     * 使用 Bulk API 写入 data stream。
     *
     * @param documents 已路由文档
     * @return Bulk 写入结果
     */
    @Override
    public BulkWriteResult write(List<RoutedLogDocument> documents) {
        if (documents == null || documents.isEmpty()) {
            return BulkWriteResult.success(0);
        }
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
            flushCount.incrementAndGet();
            return toResult(response, documents);
        } catch (IOException exception) {
            failureCount.addAndGet(documents.size());
            log.warn("Failed to bulk write logs to Elaticsearch", exception);
            return BulkWriteResult.failedAll(documents, exception.getMessage());
        }
    }

    private BulkWriteResult toResult(BulkResponse response, List<RoutedLogDocument> documents) {
        List<String> failures = new ArrayList<>();
        List<BulkItemFailure> itemFailures = new ArrayList<>();
        if (response.errors()) {
            for (int index = 0; index < response.items().size(); index++) {
                BulkResponseItem item = response.items().get(index);
                if (item.error() != null) {
                    BulkItemFailure failure = toFailure(index, item, documents);
                    itemFailures.add(failure);
                    failures.add(failure.summary());
                }
            }
        }
        int failed = itemFailures.size();
        int succeeded = documents.size() - failed;
        successCount.addAndGet(succeeded);
        failureCount.addAndGet(failed);
        if (failed > 0) {
            log.warn("Bulk write completed with failures successCount={} failureCount={}", succeeded, failed);
        }
        return new BulkWriteResult(succeeded, failed, failures, itemFailures);
    }

    private BulkItemFailure toFailure(int index, BulkResponseItem item, List<RoutedLogDocument> documents) {
        RoutedLogDocument document = index < documents.size() ? documents.get(index) : null;
        String dataStreamName = document == null ? item.index() : document.dataStreamName();
        String documentId = document == null ? item.id() : document.document().getId();
        String errorType = item.error().type();
        Integer status = item.status();
        return new BulkItemFailure(
                index,
                dataStreamName,
                documentId,
                status,
                errorType,
                item.error().reason(),
                BulkFailureType.classify(status, errorType));
    }
}
