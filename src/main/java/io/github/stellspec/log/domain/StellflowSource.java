package io.github.stellspec.log.domain;

/** Stellflow 来源元数据。 */
public record StellflowSource(String topic, int partition, long offset, String messageKey) {}
