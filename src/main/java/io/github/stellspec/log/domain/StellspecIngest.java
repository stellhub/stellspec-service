package io.github.stellspec.log.domain;

import java.time.Instant;

/** StellSpec 摄取元数据。 */
public record StellspecIngest(
        Instant ingestedAt,
        boolean sampled,
        boolean truncated,
        Integer originalLength,
        String messagePreview,
        String messageHash,
        String policy,
        String rawPayload) {}
