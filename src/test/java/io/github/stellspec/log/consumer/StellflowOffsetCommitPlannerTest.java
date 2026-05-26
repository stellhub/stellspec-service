package io.github.stellspec.log.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.stellhub.stellflow.sdk.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class StellflowOffsetCommitPlannerTest {

    private final StellflowOffsetCommitPlanner planner = new StellflowOffsetCommitPlanner();

    @Test
    void planNextOffsetPerTopicPartition() {
        List<ConsumerRecord> records =
                List.of(
                        record("logs", 0, 10),
                        record("logs", 0, 12),
                        record("logs", 1, 3),
                        record("audit", 0, 7));

        List<OffsetCommitPlan> plans = planner.plan(records);

        assertThat(plans)
                .containsExactly(
                        new OffsetCommitPlan("audit", 0, 8),
                        new OffsetCommitPlan("logs", 0, 13),
                        new OffsetCommitPlan("logs", 1, 4));
    }

    private ConsumerRecord record(String topic, int partition, long offset) {
        return new ConsumerRecord(
                topic,
                partition,
                offset,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8),
                1L);
    }
}
