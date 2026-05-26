package io.github.stellspec.log.classify;

import io.github.stellspec.config.StellspecLogProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Data stream 名称解析器。 */
@Component
@RequiredArgsConstructor
public class DataStreamNameResolver {

    private final StellspecLogProperties properties;

    /**
     * 根据分类结果生成 data stream 名称。
     *
     * @param classification 分类结果
     * @return data stream 名称
     */
    public String resolve(LogClassificationResult classification) {
        return properties.getDataStreamType()
                + "-"
                + classification.dataset()
                + "-"
                + classification.namespace();
    }
}
