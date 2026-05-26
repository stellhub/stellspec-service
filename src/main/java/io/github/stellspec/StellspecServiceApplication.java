package io.github.stellspec;

import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.config.StellspecElaticsearchProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** StellSpec 服务启动入口。 */
@SpringBootApplication
@EnableConfigurationProperties({StellspecLogProperties.class, StellspecElaticsearchProperties.class})
public class StellspecServiceApplication {

    /**
     * 启动 StellSpec 日志消费服务。
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication.run(StellspecServiceApplication.class, args);
    }
}
