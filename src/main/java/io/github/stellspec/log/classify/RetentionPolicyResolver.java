package io.github.stellspec.log.classify;

import io.github.stellspec.config.StellspecElaticsearchProperties;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 日志保留策略解析器。 */
@Component
@RequiredArgsConstructor
public class RetentionPolicyResolver {

    private final StellspecElaticsearchProperties properties;

    /**
     * 根据日志分类解析保留策略。
     *
     * @param category 日志分类
     * @return 保留策略
     */
    public RetentionPolicy resolve(LogCategory category) {
        if (category == null) {
            return RetentionPolicy.DEFAULT;
        }
        return switch (category) {
            case ACCESS, DEBUG -> RetentionPolicy.SHORT;
            case ERROR -> RetentionPolicy.ERROR;
            case AUDIT, SECURITY -> RetentionPolicy.AUDIT;
            case AGGREGATE -> RetentionPolicy.AGGREGATE;
            case APPLICATION, SLOW_LOG -> RetentionPolicy.DEFAULT;
        };
    }

    /**
     * 获取保留策略对应的 lifecycle policy 名称。
     *
     * @param retentionPolicy 保留策略
     * @return lifecycle policy 名称
     */
    public String lifecyclePolicyName(RetentionPolicy retentionPolicy) {
        return switch (retentionPolicy == null ? RetentionPolicy.DEFAULT : retentionPolicy) {
            case SHORT -> properties.getShortLifecyclePolicyName();
            case ERROR -> properties.getErrorLifecyclePolicyName();
            case AUDIT -> properties.getAuditLifecyclePolicyName();
            case AGGREGATE -> properties.getAggregateLifecyclePolicyName();
            case DEFAULT -> properties.getDefaultLifecyclePolicyName();
        };
    }

    /**
     * 获取保留策略对应的 data stream pattern。
     *
     * @param retentionPolicy 保留策略
     * @return data stream pattern 集合
     */
    public List<String> indexPatterns(RetentionPolicy retentionPolicy) {
        return switch (retentionPolicy == null ? RetentionPolicy.DEFAULT : retentionPolicy) {
            case SHORT -> properties.getShortIndexPatterns();
            case ERROR -> properties.getErrorIndexPatterns();
            case AUDIT -> properties.getAuditIndexPatterns();
            case AGGREGATE -> properties.getAggregateIndexPatterns();
            case DEFAULT -> List.of(properties.getIndexPattern());
        };
    }
}
