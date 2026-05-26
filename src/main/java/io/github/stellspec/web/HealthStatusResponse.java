package io.github.stellspec.web;

import java.util.Map;

/** 健康检查响应。 */
public record HealthStatusResponse(String status, Map<String, Object> details) {}
