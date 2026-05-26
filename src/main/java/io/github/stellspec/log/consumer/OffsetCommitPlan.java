package io.github.stellspec.log.consumer;

/** Offset 提交计划。 */
public record OffsetCommitPlan(String topic, int partition, long nextOffset) {}
