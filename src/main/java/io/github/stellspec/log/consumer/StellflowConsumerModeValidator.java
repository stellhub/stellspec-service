package io.github.stellspec.log.consumer;

import io.github.stellflux.stellflow.StellfluxStellflowProperties;
import io.github.stellspec.config.StellspecLogProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Stellflow 消费模式互斥校验器。 */
@Component
@RequiredArgsConstructor
public class StellflowConsumerModeValidator implements ApplicationRunner {

    private final StellspecLogProperties logProperties;

    private final StellfluxStellflowProperties stellflowProperties;

    /**
     * 校验独立 bulk consumer 与注解 listener 不会同时启用。
     *
     * @param args 启动参数
     */
    @Override
    public void run(ApplicationArguments args) {
        boolean bulkConsumerEnabled = logProperties.getBulkConsumer().isEnabled();
        boolean listenerEnabled = stellflowProperties.getConsumer().isListenerEnabled()
                && stellflowProperties.getConsumer().isListenerAutoStartup();
        if (bulkConsumerEnabled && listenerEnabled) {
            throw new IllegalStateException(
                    "Stellflow bulk consumer and @StellflowListener cannot be enabled together. "
                            + "Set STELLSPEC_STELLFLOW_LISTENER_ENABLED=false when "
                            + "STELLSPEC_BULK_CONSUMER_ENABLED=true.");
        }
    }
}
