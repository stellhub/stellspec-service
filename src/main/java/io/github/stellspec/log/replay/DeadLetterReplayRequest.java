package io.github.stellspec.log.replay;

import java.time.Instant;

/** Dead letter replay 请求。 */
public record DeadLetterReplayRequest(
        Instant from,
        Instant to,
        String errorType,
        String topic,
        int limit,
        boolean dryRun) {

    /**
     * 解析安全 limit。
     *
     * @return 查询上限
     */
    public int safeLimit() {
        if (limit <= 0) {
            return 100;
        }
        return Math.min(limit, 5000);
    }
}
