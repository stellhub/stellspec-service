package io.github.stellspec.log.classify;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.stellspec.config.StellspecElaticsearchProperties;
import org.junit.jupiter.api.Test;

class RetentionPolicyResolverTest {

    private final RetentionPolicyResolver resolver =
            new RetentionPolicyResolver(new StellspecElaticsearchProperties());

    @Test
    void mapLogCategoryToRetentionPolicy() {
        assertThat(resolver.resolve(LogCategory.ACCESS)).isEqualTo(RetentionPolicy.SHORT);
        assertThat(resolver.resolve(LogCategory.DEBUG)).isEqualTo(RetentionPolicy.SHORT);
        assertThat(resolver.resolve(LogCategory.ERROR)).isEqualTo(RetentionPolicy.ERROR);
        assertThat(resolver.resolve(LogCategory.AUDIT)).isEqualTo(RetentionPolicy.AUDIT);
        assertThat(resolver.resolve(LogCategory.SECURITY)).isEqualTo(RetentionPolicy.AUDIT);
        assertThat(resolver.resolve(LogCategory.AGGREGATE)).isEqualTo(RetentionPolicy.AGGREGATE);
        assertThat(resolver.resolve(LogCategory.APPLICATION)).isEqualTo(RetentionPolicy.DEFAULT);
    }

    @Test
    void resolveLifecyclePolicyNameAndPatterns() {
        assertThat(resolver.lifecyclePolicyName(RetentionPolicy.SHORT))
                .isEqualTo("stellspec-logs-short-policy");
        assertThat(resolver.lifecyclePolicyName(RetentionPolicy.ERROR))
                .isEqualTo("stellspec-logs-error-policy");
        assertThat(resolver.lifecyclePolicyName(RetentionPolicy.AUDIT))
                .isEqualTo("stellspec-logs-audit-policy");
        assertThat(resolver.lifecyclePolicyName(RetentionPolicy.AGGREGATE))
                .isEqualTo("stellspec-logs-aggregate-policy");
        assertThat(resolver.indexPatterns(RetentionPolicy.SHORT))
                .containsExactly("logs-*-access-*", "logs-*-debug-*");
    }
}
