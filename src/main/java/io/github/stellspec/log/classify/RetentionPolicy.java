package io.github.stellspec.log.classify;

/** 日志保留策略。 */
public enum RetentionPolicy {
    SHORT,
    DEFAULT,
    ERROR,
    AUDIT,
    AGGREGATE
}
