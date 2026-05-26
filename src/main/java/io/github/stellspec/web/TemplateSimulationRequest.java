package io.github.stellspec.web;

import jakarta.validation.constraints.NotBlank;

/** Template simulation 请求。 */
public record TemplateSimulationRequest(@NotBlank String dataStreamName) {}
