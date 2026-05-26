package io.github.stellspec.log.consumer;

import io.github.stellhub.stellflow.sdk.consumer.ConsumerRecord;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Stellflow offset 精确提交计划器。 */
@Component
public class StellflowOffsetCommitPlanner {

    /**
     * 按 topic/partition 计算下一次应提交的 offset。
     *
     * @param records 已成功处理的消费记录
     * @return offset 提交计划
     */
    public List<OffsetCommitPlan> plan(List<ConsumerRecord> records) {
        Map<TopicPartitionKey, Long> maxOffsets = new LinkedHashMap<>();
        records.stream()
                .sorted(Comparator.comparing(ConsumerRecord::topic)
                        .thenComparingInt(ConsumerRecord::partition)
                        .thenComparingLong(ConsumerRecord::offset))
                .forEach(record ->
                        maxOffsets.merge(
                                new TopicPartitionKey(record.topic(), record.partition()),
                                record.offset(),
                                Math::max));
        return maxOffsets.entrySet().stream()
                .map(entry ->
                        new OffsetCommitPlan(
                                entry.getKey().topic(),
                                entry.getKey().partition(),
                                entry.getValue() + 1L))
                .toList();
    }

    private record TopicPartitionKey(String topic, int partition) {}
}
