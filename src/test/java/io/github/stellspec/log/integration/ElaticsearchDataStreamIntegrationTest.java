package io.github.stellspec.log.integration;

import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import io.github.stellspec.config.StellspecElaticsearchProperties;
import io.github.stellspec.log.domain.EcsEvent;
import io.github.stellspec.log.domain.EcsLog;
import io.github.stellspec.log.domain.EcsLogDocument;
import io.github.stellspec.log.domain.EcsService;
import io.github.stellspec.log.domain.RoutedLogDocument;
import io.github.stellspec.log.template.ElaticsearchTemplateManager;
import io.github.stellspec.log.writer.ElaticsearchBulkLogWriter;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class ElaticsearchDataStreamIntegrationTest {

    @Container
    private static final ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.18.8")
                    .withEnv("discovery.type", "single-node")
                    .withEnv("xpack.security.enabled", "false");

    @Test
    void bootstrapTemplateAndBulkWriteDataStream() throws Exception {
        try (RestClient restClient =
                        RestClient.builder(HttpHost.create(ELASTICSEARCH.getHttpHostAddress())).build();
                RestClientTransport transport =
                        new RestClientTransport(restClient, new JacksonJsonpMapper())) {
            ElasticsearchClient client = new ElasticsearchClient(transport);
            StellspecElaticsearchProperties properties = new StellspecElaticsearchProperties();
            properties.setBootstrapEnabled(true);
            properties.setBootstrapMode(StellspecElaticsearchProperties.BootstrapMode.OVERWRITE);
            properties.setOverwriteTemplates(true);
            new ElaticsearchTemplateManager(restClient, properties).bootstrap();

            EcsLogDocument document =
                    EcsLogDocument.builder()
                            .id("test-1")
                            .dataStreamName("logs-test-service-prod")
                            .timestamp(Instant.parse("2026-05-26T01:02:03Z"))
                            .message("hello data stream")
                            .log(new EcsLog("INFO", "demo.Logger"))
                            .service(new EcsService("test-service", "1.0.0", "prod"))
                            .event(
                                    new EcsEvent(
                                            "test-service",
                                            "event",
                                            List.of("application"),
                                            List.of("info"),
                                            "hash-1"))
                            .attributes(Map.of("order.id", "10001"))
                            .build();

            ElaticsearchBulkLogWriter writer = new ElaticsearchBulkLogWriter(client);
            writer.write(List.of(new RoutedLogDocument("logs-test-service-prod", document)));
            client.indices().refresh(refresh -> refresh.index("logs-test-service-prod"));

            var response =
                    client.search(
                            search -> search.index("logs-test-service-prod"),
                            Map.class);

            assertThat(response.hits().total().value()).isEqualTo(1L);
            assertThat(writer.getSuccessCount().get()).isEqualTo(1L);
        }
    }
}
