package io.github.stellspec.log.merge;

import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.StellspecIngest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 重复日志窗口合并器。 */
@Component
@RequiredArgsConstructor
public class LogMergeWindowAggregator {

    private final StellspecLogProperties properties;

    private final Map<String, WindowState> windows = new ConcurrentHashMap<>();

    /**
     * 处理重复日志并返回需要写入的文档。
     *
     * @param document ECS 日志文档
     * @return 需要写入的文档集合
     */
    public List<EcsLogDocument> aggregate(EcsLogDocument document) {
        if (!properties.getMerge().isEnabled()
                || document.getEvent() == null
                || document.getEvent().hash() == null) {
            return List.of(document);
        }
        WindowState state =
                windows.compute(
                        document.getEvent().hash(),
                        (hash, current) -> nextState(hash, current, document));
        if (state == null) {
            return List.of(document);
        }
        if (state.count <= properties.getMerge().getDuplicateThreshold()) {
            return List.of(document);
        }
        if (state.count % properties.getMerge().getDuplicateThreshold() == 0) {
            return List.of(aggregateDocument(state, document));
        }
        return List.of();
    }

    private WindowState nextState(String hash, WindowState current, EcsLogDocument document) {
        Instant now = document.getTimestamp() == null ? Instant.now() : document.getTimestamp();
        long windowMillis = properties.getMerge().getWindowMillis();
        if (current == null || now.toEpochMilli() - current.windowStart.toEpochMilli() >= windowMillis) {
            return new WindowState(hash, now, now, now, 1, document);
        }
        current.lastSeen = now;
        current.count++;
        current.lastSample = document;
        return current;
    }

    private EcsLogDocument aggregateDocument(WindowState state, EcsLogDocument latest) {
        Map<String, Object> attributes =
                Map.of(
                        "stellspec.merge.window_start",
                        state.windowStart.toString(),
                        "stellspec.merge.window_end",
                        state.lastSeen.toString(),
                        "stellspec.merge.occurrence_count",
                        state.count,
                        "stellspec.merge.sample_message",
                        latest.getMessage());
        EcsEvent event =
                new EcsEvent(
                        latest.getEvent().dataset(),
                        "metric",
                        List.of("application"),
                        List.of("aggregation"),
                        state.hash);
        StellspecIngest ingest =
                new StellspecIngest(
                        Instant.now(), true, false, null, null, null, "merge-window", null);
        return latest.withId("aggregate-" + state.hash + "-" + state.windowStart.toEpochMilli())
                .withMessage("Aggregated duplicate log events")
                .withEvent(event)
                .withAttributes(attributes)
                .withStellspecIngest(ingest);
    }

    private static final class WindowState {
        private final String hash;
        private final Instant windowStart;
        private Instant firstSeen;
        private Instant lastSeen;
        private int count;
        private EcsLogDocument lastSample;

        private WindowState(
                String hash,
                Instant windowStart,
                Instant firstSeen,
                Instant lastSeen,
                int count,
                EcsLogDocument lastSample) {
            this.hash = hash;
            this.windowStart = windowStart;
            this.firstSeen = firstSeen;
            this.lastSeen = lastSeen;
            this.count = count;
            this.lastSample = lastSample;
        }
    }
}
