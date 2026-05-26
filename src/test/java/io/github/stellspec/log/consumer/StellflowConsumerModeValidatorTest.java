package io.github.stellspec.log.consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.stellflux.stellflow.StellfluxStellflowProperties;
import io.github.stellspec.config.StellspecLogProperties;
import org.junit.jupiter.api.Test;

class StellflowConsumerModeValidatorTest {

    @Test
    void failWhenBulkConsumerAndListenerAreBothEnabled() {
        StellspecLogProperties logProperties = new StellspecLogProperties();
        logProperties.getBulkConsumer().setEnabled(true);
        StellfluxStellflowProperties stellflowProperties = new StellfluxStellflowProperties();
        stellflowProperties.getConsumer().setListenerEnabled(true);
        stellflowProperties.getConsumer().setListenerAutoStartup(true);

        StellflowConsumerModeValidator validator =
                new StellflowConsumerModeValidator(logProperties, stellflowProperties);

        assertThatThrownBy(() -> validator.run(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be enabled together");
    }

    @Test
    void passWhenBulkConsumerIsDisabled() {
        StellspecLogProperties logProperties = new StellspecLogProperties();
        StellfluxStellflowProperties stellflowProperties = new StellfluxStellflowProperties();
        stellflowProperties.getConsumer().setListenerEnabled(true);
        stellflowProperties.getConsumer().setListenerAutoStartup(true);

        StellflowConsumerModeValidator validator =
                new StellflowConsumerModeValidator(logProperties, stellflowProperties);

        assertThatCode(() -> validator.run(null)).doesNotThrowAnyException();
    }
}
