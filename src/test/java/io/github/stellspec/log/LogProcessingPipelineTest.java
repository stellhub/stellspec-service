package io.github.stellspec.log;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecElaticsearchProperties;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.classify.DataStreamNameResolver;
import io.github.stellspec.log.classify.LogClassifier;
import io.github.stellspec.log.classify.RetentionPolicyResolver;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.filter.LogFilterChain;
import io.github.stellspec.log.merge.LogFingerprintGenerator;
import io.github.stellspec.log.merge.LogMergeWindowAggregator;
import io.github.stellspec.log.normalize.EcsLogNormalizer;
import io.github.stellspec.log.validation.LogSchemaValidator;
import java.util.List;
import org.junit.jupiter.api.Test;

class LogProcessingPipelineTest {

    private final StellspecLogProperties properties = new StellspecLogProperties();

    private final RetentionPolicyResolver retentionPolicyResolver =
            new RetentionPolicyResolver(new StellspecElaticsearchProperties());

    private final LogProcessingPipeline pipeline =
            new LogProcessingPipeline(
                    new EcsLogNormalizer(new ObjectMapper(), properties),
                    new LogFilterChain(properties),
                    new LogFingerprintGenerator(),
                    new LogMergeWindowAggregator(properties),
                    new LogClassifier(properties, retentionPolicyResolver),
                    new DataStreamNameResolver(properties),
                    new LogSchemaValidator());

    @Test
    void routeErrorLogToErrorDataStream() {
        StellflowMessage message =
                StellflowMessage.ofString(
                        "stellspec.logs",
                        "payment",
                        """
                        {
                          "timestamp": "2026-05-26T01:02:03Z",
                          "severityText": "ERROR",
                          "serviceName": "payment-service",
                          "body": "payment failed orderId=10001",
                          "exception": {
                            "type": "java.lang.IllegalStateException",
                            "message": "inventory unavailable",
                            "stacktrace": "java.lang.IllegalStateException: inventory unavailable\\n\\tat demo.App.run(App.java:12)"
                          }
                        }
                        """);

        List<RoutedLogDocument> documents = pipeline.process(message);

        assertThat(documents).hasSize(1);
        RoutedLogDocument routed = documents.getFirst();
        assertThat(routed.dataStreamName()).isEqualTo("logs-payment-service-error-prod");
        assertThat(routed.document().getEvent().hash()).isNotBlank();
        assertThat(routed.document().getError().type()).isEqualTo("java.lang.IllegalStateException");
        assertThat(routed.document().toSource())
                .extractingByKey("message")
                .isEqualTo("payment failed orderId=10001");
    }

    @Test
    void dropHealthCheckAccessLog() {
        StellflowMessage message =
                StellflowMessage.ofString(
                        "stellspec.logs",
                        "gateway",
                        """
                        {
                          "timestamp": "2026-05-26T01:02:03Z",
                          "severityText": "INFO",
                          "serviceName": "gateway",
                          "body": "GET /actuator/health",
                          "attributes": {
                            "http.request.method": "GET",
                            "url.path": "/actuator/health"
                          }
                        }
                        """);

        assertThat(pipeline.process(message)).isEmpty();
    }

    @Test
    void aggregateDuplicateErrorsAfterThreshold() {
        properties.getMerge().setDuplicateThreshold(2);
        StellflowMessage message =
                StellflowMessage.ofString(
                        "stellspec.logs",
                        "order",
                        """
                        {
                          "timestamp": "2026-05-26T01:02:03Z",
                          "severityText": "ERROR",
                          "serviceName": "order-service",
                          "body": "create order failed orderId=10001",
                          "exception": {
                            "type": "java.lang.NullPointerException",
                            "message": "user is null",
                            "stacktrace": "java.lang.NullPointerException: user is null\\n\\tat demo.Order.run(Order.java:12)"
                          }
                        }
                        """);

        assertThat(pipeline.process(message)).hasSize(1);
        assertThat(pipeline.process(message)).hasSize(1);
        List<RoutedLogDocument> third = pipeline.process(message);

        assertThat(third).isEmpty();
        List<RoutedLogDocument> fourth = pipeline.process(message);
        assertThat(fourth).hasSize(1);
        assertThat(fourth.getFirst().dataStreamName()).isEqualTo("logs-stellspec-aggregate-prod");
        assertThat(fourth.getFirst().document().getEvent().kind()).isEqualTo("metric");
    }
}
