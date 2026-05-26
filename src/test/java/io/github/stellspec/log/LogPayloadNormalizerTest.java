package io.github.stellspec.log;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.LogDocument;
import org.junit.jupiter.api.Test;

class LogPayloadNormalizerTest {

    private final StellspecLogProperties properties = new StellspecLogProperties();

    private final LogPayloadNormalizer normalizer =
            new LogPayloadNormalizer(new ObjectMapper(), properties);

    @Test
    void normalizeFlatJsonPayload() {
        StellflowMessage message =
                StellflowMessage.ofString(
                        "stellspec.logs",
                        "order-service",
                        """
                        {
                          "timestamp": "2026-05-25T01:02:03Z",
                          "severityText": "INFO",
                          "traceId": "trace-1",
                          "spanId": "span-1",
                          "serviceName": "order-service",
                          "body": "order created",
                          "attributes": {
                            "tenant.id": "t1"
                          }
                        }
                        """);

        LogDocument document = normalizer.normalize(message);

        assertThat(document.getIndexName()).isEqualTo("stellspec-logs-2026.05.25");
        assertThat(document.getSeverityText()).isEqualTo("INFO");
        assertThat(document.getTraceId()).isEqualTo("trace-1");
        assertThat(document.getSpanId()).isEqualTo("span-1");
        assertThat(document.getServiceName()).isEqualTo("order-service");
        assertThat(document.getBody()).isEqualTo("order created");
        assertThat(document.getAttributes()).containsEntry("tenant.id", "t1");
    }

    @Test
    void normalizeOtelResourceLogsPayload() {
        StellflowMessage message =
                StellflowMessage.ofString(
                        "stellspec.logs",
                        null,
                        """
                        {
                          "resourceLogs": [
                            {
                              "resource": {
                                "attributes": [
                                  {
                                    "key": "service.name",
                                    "value": {"stringValue": "billing-service"}
                                  }
                                ]
                              },
                              "scopeLogs": [
                                {
                                  "logRecords": [
                                    {
                                      "timeUnixNano": "1779667200000000000",
                                      "severityText": "ERROR",
                                      "body": {"stringValue": "payment failed"},
                                      "attributes": [
                                        {
                                          "key": "error.type",
                                          "value": {"stringValue": "PaymentException"}
                                        }
                                      ]
                                    }
                                  ]
                                }
                              ]
                            }
                          ]
                        }
                        """);

        LogDocument document = normalizer.normalize(message);

        assertThat(document.getIndexName()).isEqualTo("stellspec-logs-2026.05.25");
        assertThat(document.getSeverityText()).isEqualTo("ERROR");
        assertThat(document.getServiceName()).isEqualTo("billing-service");
        assertThat(document.getBody()).isEqualTo("payment failed");
        assertThat(document.getAttributes()).containsEntry("error.type", "PaymentException");
    }
}
