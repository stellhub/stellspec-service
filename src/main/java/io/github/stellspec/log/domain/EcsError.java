package io.github.stellspec.log.domain;

/** ECS error 字段组。 */
public record EcsError(String type, String message, String stackTrace, String code) {}
