package io.github.stellspec.log.replay;

/** Dead letter replay 响应。 */
public record DeadLetterReplayResponse(
        int matchedCount,
        int replayedCount,
        int failedCount,
        int skippedCount,
        boolean dryRun) {}
