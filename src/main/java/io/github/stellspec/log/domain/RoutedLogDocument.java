package io.github.stellspec.log.domain;

/** 已路由的日志文档。 */
public record RoutedLogDocument(String dataStreamName, EcsLogDocument document) {}
