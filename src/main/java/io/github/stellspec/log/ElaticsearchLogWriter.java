package io.github.stellspec.log;

import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import io.github.stellflux.elaticsearch.StellfluxElaticsearchClient;
import io.github.stellspec.log.domain.LogDocument;
import java.io.IOException;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Elaticsearch 日志写入器。 */
@Component
@RequiredArgsConstructor
public class ElaticsearchLogWriter {

    private final StellfluxElaticsearchClient elaticsearchClient;

    /**
     * 写入日志文档。
     *
     * @param document 日志文档
     * @return 写入响应
     * @throws IOException Elaticsearch 写入异常
     */
    public IndexResponse write(LogDocument document) throws IOException {
        IndexRequest<Map<String, Object>> request =
                IndexRequest.of(
                        builder ->
                                builder.index(document.getIndexName())
                                        .id(document.getId())
                                        .document(document.toSource()));
        return elaticsearchClient.index(request);
    }
}
