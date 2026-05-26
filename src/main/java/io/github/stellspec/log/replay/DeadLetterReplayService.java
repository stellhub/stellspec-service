package io.github.stellspec.log.replay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.LogProcessingPipeline;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.writer.BulkFailureHandler;
import io.github.stellspec.log.writer.BulkFailureHandlingResult;
import io.github.stellspec.log.writer.BulkLogBuffer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

/** Dead letter replay 服务。 */
@Service
@RequiredArgsConstructor
public class DeadLetterReplayService {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final RestClient restClient;

    private final ObjectMapper objectMapper;

    private final StellspecLogProperties properties;

    private final LogProcessingPipeline pipeline;

    private final BulkLogBuffer bulkLogBuffer;

    private final BulkFailureHandler bulkFailureHandler;

    /**
     * 查询 dead letter 并重新走当前摄取链路。
     *
     * @param replayRequest replay 请求
     * @return replay 结果
     */
    public DeadLetterReplayResponse replay(DeadLetterReplayRequest replayRequest) {
        DeadLetterReplayRequest safeRequest = replayRequest == null
                ? new DeadLetterReplayRequest(null, null, null, null, 100, true)
                : replayRequest;
        List<Map<String, Object>> sources = searchDeadLetters(safeRequest);
        if (safeRequest.dryRun()) {
            return new DeadLetterReplayResponse(sources.size(), 0, 0, 0, true);
        }
        int replayed = 0;
        int failed = 0;
        int skipped = 0;
        for (Map<String, Object> source : sources) {
            StellflowMessage message = toMessage(source);
            if (message == null) {
                skipped++;
                continue;
            }
            try {
                List<RoutedLogDocument> documents = pipeline.process(message);
                BulkWriteResult result = bulkLogBuffer.writeNow(documents);
                BulkFailureHandlingResult handlingResult =
                        bulkFailureHandler.handle(documents, result, List.of(message), "dead-letter-replay");
                if (result.failureCount() == 0 || handlingResult.fullyHandled()) {
                    replayed++;
                } else {
                    failed++;
                }
            } catch (Throwable throwable) {
                failed++;
            }
        }
        return new DeadLetterReplayResponse(sources.size(), replayed, failed, skipped, false);
    }

    private List<Map<String, Object>> searchDeadLetters(DeadLetterReplayRequest replayRequest) {
        Request request = new Request(
                "POST",
                "/" + properties.getBulkConsumer().getDeadLetterDataStream() + "/_search");
        request.setEntity(new StringEntity(toJson(searchBody(replayRequest)), ContentType.APPLICATION_JSON));
        try {
            Response response = restClient.performRequest(request);
            String json = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            Map<String, Object> root = objectMapper.readValue(json, MAP_TYPE);
            return extractSources(root);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to search dead letter data stream", exception);
        }
    }

    private Map<String, Object> searchBody(DeadLetterReplayRequest replayRequest) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("size", replayRequest.safeLimit());
        body.put("sort", List.of(Map.of("@timestamp", Map.of("order", "asc"))));
        List<Map<String, Object>> filters = new ArrayList<>();
        rangeFilter(filters, replayRequest.from(), replayRequest.to());
        termFilter(filters, "attributes.stellspec.deadletter.error_type", replayRequest.errorType());
        termFilter(filters, "stellflow.topic", replayRequest.topic());
        if (filters.isEmpty()) {
            body.put("query", Map.of("match_all", Map.of()));
        } else {
            body.put("query", Map.of("bool", Map.of("filter", filters)));
        }
        return body;
    }

    private void rangeFilter(List<Map<String, Object>> filters, Instant from, Instant to) {
        if (from == null && to == null) {
            return;
        }
        Map<String, Object> range = new LinkedHashMap<>();
        if (from != null) {
            range.put("gte", from.toString());
        }
        if (to != null) {
            range.put("lte", to.toString());
        }
        filters.add(Map.of("range", Map.of("@timestamp", range)));
    }

    private void termFilter(List<Map<String, Object>> filters, String field, String value) {
        if (value != null && !value.isBlank()) {
            filters.add(Map.of("term", Map.of(field, value)));
        }
    }

    private List<Map<String, Object>> extractSources(Map<String, Object> root) {
        Map<String, Object> hits = mapValue(root.get("hits"));
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object hit : listValue(hits.get("hits"))) {
            Map<String, Object> hitMap = mapValue(hit);
            Map<String, Object> source = mapValue(hitMap.get("_source"));
            if (!source.isEmpty()) {
                result.add(source);
            }
        }
        return result;
    }

    private StellflowMessage toMessage(Map<String, Object> source) {
        Map<String, Object> attributes = mapValue(source.get("attributes"));
        String payload = text(attributes.get("stellspec.deadletter.payload"));
        if (payload == null || payload.isBlank()) {
            return null;
        }
        Map<String, Object> stellflow = mapValue(source.get("stellflow"));
        String topic = text(stellflow.get("topic"));
        String key = text(stellflow.get("message_key"));
        return StellflowMessage.ofString(
                topic == null || topic.isBlank() ? "stellspec.logs" : topic,
                key,
                payload);
    }

    private String toJson(Map<String, Object> body) {
        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to build dead letter replay query", exception);
        }
    }

    private String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private List<?> listValue(Object value) {
        return value instanceof List<?> list ? list : List.of();
    }

    private Map<String, Object> mapValue(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }
}
