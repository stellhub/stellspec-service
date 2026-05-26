package io.github.stellspec;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(
        properties = {
            "stellflux.stellflow.consumer.listener-enabled=false",
            "stellflux.stellflow.consumer.listener-auto-startup=false"
        })
class StellspecServiceApplicationTest {

    @Test
    void contextLoads() {}
}
