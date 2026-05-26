package io.github.stellspec.log.template;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class ElaticsearchTemplateResourceTest {

    @Test
    void indexTemplateDeclaresDataStream() throws Exception {
        ClassPathResource resource =
                new ClassPathResource("elaticsearch/index-templates/stellspec-logs-template.json");

        String json = resource.getContentAsString(StandardCharsets.UTF_8);

        assertThat(json).contains("\"data_stream\"");
        assertThat(json).contains("\"logs-*-*\"");
        assertThat(json).contains("stellspec-ecs-mappings");
    }

    @Test
    void indexTemplatesBindLifecyclePoliciesByCategory() throws Exception {
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-template.json",
                "logs-*-*",
                "stellspec-logs-default-policy");
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-short-template.json",
                "logs-*-access-*",
                "stellspec-logs-short-policy");
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-short-template.json",
                "logs-*-debug-*",
                "stellspec-logs-short-policy");
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-error-template.json",
                "logs-*-error-*",
                "stellspec-logs-error-policy");
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-audit-template.json",
                "logs-audit-*",
                "stellspec-logs-audit-policy");
        assertTemplate(
                "elaticsearch/index-templates/stellspec-logs-aggregate-template.json",
                "logs-stellspec-aggregate-*",
                "stellspec-logs-aggregate-policy");
    }

    @Test
    void commonSettingsDoNotForceDefaultLifecycle() throws Exception {
        ClassPathResource resource =
                new ClassPathResource("elaticsearch/component-templates/stellspec-log-settings.json");

        String json = resource.getContentAsString(StandardCharsets.UTF_8);

        assertThat(json).doesNotContain("\"index.lifecycle.name\"");
    }

    @Test
    void ecsMappingContainsEnterpriseFields() throws Exception {
        ClassPathResource resource =
                new ClassPathResource("elaticsearch/component-templates/stellspec-ecs-mappings.json");

        String json = resource.getContentAsString(StandardCharsets.UTF_8);

        assertThat(json).contains("\"@timestamp\"");
        assertThat(json).contains("\"match_only_text\"");
        assertThat(json).contains("\"flattened\"");
        assertThat(json).contains("\"stack_trace\"");
        assertThat(json).contains("\"wildcard\"");
    }

    private void assertTemplate(String path, String pattern, String lifecyclePolicy) throws Exception {
        ClassPathResource resource = new ClassPathResource(path);

        String json = resource.getContentAsString(StandardCharsets.UTF_8);

        assertThat(json).contains("\"data_stream\"");
        assertThat(json).contains(pattern);
        assertThat(json).contains(lifecyclePolicy);
        assertThat(json).contains("stellspec-ecs-mappings");
    }
}
