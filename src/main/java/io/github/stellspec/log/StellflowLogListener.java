package io.github.stellspec.log;

import io.github.stellflux.stellflow.listener.StellflowListener;
import io.github.stellflux.stellflow.listener.StellflowListenerContext;
import io.github.stellflux.stellflow.message.StellflowMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Stellflow 日志消息监听器。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StellflowLogListener {

    private final StellspecLogIngestionService ingestionService;

    /**
     * 消费 Stellflow 日志消息并写入 Elaticsearch。
     *
     * @param message Stellflow 消息
     * @param context 监听上下文
     */
    @StellflowListener
    public void onLog(StellflowMessage message, StellflowListenerContext context) {
        log.debug(
                "Received Stellflow log groupId={} topic={} partition={} offset={}",
                context.getGroupId(),
                message.topic(),
                message.partition(),
                message.offset());
        ingestionService.ingest(message);
    }
}
