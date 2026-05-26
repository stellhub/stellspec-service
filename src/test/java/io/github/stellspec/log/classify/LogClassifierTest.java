package io.github.stellspec.log.classify;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.stellspec.config.StellspecElaticsearchProperties;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.EcsError;
import io.github.stellspec.log.domain.EcsLog;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.EcsService;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogClassifierTest {

    private final LogClassifier classifier =
            new LogClassifier(
                    new StellspecLogProperties(),
                    new RetentionPolicyResolver(new StellspecElaticsearchProperties()));

    @Test
    void classifyErrorLogWithErrorRetentionPolicy() {
        LogClassificationResult result =
                classifier.classify(
                        EcsLogDocument.builder()
                                .service(new EcsService("payment-service", null, "prod"))
                                .log(new EcsLog("ERROR", "demo.Payment"))
                                .error(new EcsError("java.lang.IllegalStateException", "failed", null, null))
                                .build());

        assertThat(result.category()).isEqualTo(LogCategory.ERROR);
        assertThat(result.dataset()).isEqualTo("payment-service-error");
        assertThat(result.retentionPolicy()).isEqualTo(RetentionPolicy.ERROR);
    }

    @Test
    void classifyAccessAndDebugLogWithShortRetentionPolicy() {
        LogClassificationResult access =
                classifier.classify(
                        EcsLogDocument.builder()
                                .service(new EcsService("gateway", null, "prod"))
                                .attributes(Map.of("http.request.method", "GET", "url.path", "/orders"))
                                .build());
        LogClassificationResult debug =
                classifier.classify(
                        EcsLogDocument.builder()
                                .service(new EcsService("order-service", null, "prod"))
                                .log(new EcsLog("DEBUG", "demo.Order"))
                                .build());

        assertThat(access.retentionPolicy()).isEqualTo(RetentionPolicy.SHORT);
        assertThat(access.dataset()).isEqualTo("gateway-access");
        assertThat(debug.retentionPolicy()).isEqualTo(RetentionPolicy.SHORT);
        assertThat(debug.dataset()).isEqualTo("order-service-debug");
    }

    @Test
    void classifyAuditLogWithAuditRetentionPolicy() {
        LogClassificationResult result =
                classifier.classify(
                        EcsLogDocument.builder()
                                .service(new EcsService("iam", null, "prod"))
                                .attributes(Map.of("audit.action", "grant-role"))
                                .build());

        assertThat(result.category()).isEqualTo(LogCategory.AUDIT);
        assertThat(result.dataset()).isEqualTo("audit");
        assertThat(result.retentionPolicy()).isEqualTo(RetentionPolicy.AUDIT);
    }
}
