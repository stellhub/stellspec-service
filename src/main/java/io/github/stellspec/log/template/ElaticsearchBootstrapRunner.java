package io.github.stellspec.log.template;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Elaticsearch 启动治理执行器。 */
@Component
@RequiredArgsConstructor
public class ElaticsearchBootstrapRunner implements ApplicationRunner {

    private final ElaticsearchTemplateManager templateManager;

    /**
     * 应用启动后执行 Elaticsearch bootstrap。
     *
     * @param args 启动参数
     */
    @Override
    public void run(ApplicationArguments args) {
        templateManager.bootstrap();
    }
}
