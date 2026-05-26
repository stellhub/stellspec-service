package io.github.stellspec.log.domain;

import java.util.List;

/** ECS event 字段组。 */
public record EcsEvent(
        String dataset, String kind, List<String> category, List<String> type, String hash) {}
