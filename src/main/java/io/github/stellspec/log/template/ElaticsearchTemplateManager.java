package io.github.stellspec.log.template;

import io.github.stellspec.config.StellspecElaticsearchProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/** Elaticsearch 模板与生命周期管理器。 */
@Component
@RequiredArgsConstructor
public class ElaticsearchTemplateManager {

    private final RestClient restClient;

    private final StellspecElaticsearchProperties properties;

    /**
     * 创建或校验日志 data stream 所需资源。
     */
    public void bootstrap() {
        StellspecElaticsearchProperties.BootstrapMode mode = properties.getBootstrapMode();
        if (!properties.isBootstrapEnabled()
                || mode == StellspecElaticsearchProperties.BootstrapMode.DISABLED) {
            return;
        }
        if (mode == StellspecElaticsearchProperties.BootstrapMode.VALIDATE_ONLY) {
            validate();
            return;
        }
        boolean overwrite =
                properties.isOverwriteTemplates()
                        || mode == StellspecElaticsearchProperties.BootstrapMode.OVERWRITE;
        putIfNeeded(
                "/_component_template/" + properties.getEcsComponentTemplateName(),
                "elaticsearch/component-templates/stellspec-ecs-mappings.json",
                overwrite);
        putIfNeeded(
                "/_component_template/" + properties.getSettingsComponentTemplateName(),
                "elaticsearch/component-templates/stellspec-log-settings.json",
                overwrite);
        putIfNeeded(
                "/_ilm/policy/" + properties.getDefaultLifecyclePolicyName(),
                "elaticsearch/lifecycle/stellspec-logs-default-policy.json",
                overwrite);
        putIfNeeded(
                "/_ilm/policy/" + properties.getShortLifecyclePolicyName(),
                "elaticsearch/lifecycle/stellspec-logs-short-policy.json",
                overwrite);
        putIfNeeded(
                "/_ilm/policy/" + properties.getErrorLifecyclePolicyName(),
                "elaticsearch/lifecycle/stellspec-logs-error-policy.json",
                overwrite);
        putIfNeeded(
                "/_ilm/policy/" + properties.getAuditLifecyclePolicyName(),
                "elaticsearch/lifecycle/stellspec-logs-audit-policy.json",
                overwrite);
        putIfNeeded(
                "/_ilm/policy/" + properties.getAggregateLifecyclePolicyName(),
                "elaticsearch/lifecycle/stellspec-logs-aggregate-policy.json",
                overwrite);
        putIfNeeded(
                "/_index_template/" + properties.getIndexTemplateName(),
                "elaticsearch/index-templates/stellspec-logs-template.json",
                overwrite);
        putIfNeeded(
                "/_index_template/" + properties.getShortIndexTemplateName(),
                "elaticsearch/index-templates/stellspec-logs-short-template.json",
                overwrite);
        putIfNeeded(
                "/_index_template/" + properties.getErrorIndexTemplateName(),
                "elaticsearch/index-templates/stellspec-logs-error-template.json",
                overwrite);
        putIfNeeded(
                "/_index_template/" + properties.getAuditIndexTemplateName(),
                "elaticsearch/index-templates/stellspec-logs-audit-template.json",
                overwrite);
        putIfNeeded(
                "/_index_template/" + properties.getAggregateIndexTemplateName(),
                "elaticsearch/index-templates/stellspec-logs-aggregate-template.json",
                overwrite);
    }

    /**
     * 校验模板资源是否存在。
     */
    public void validate() {
        requireExists("/_component_template/" + properties.getEcsComponentTemplateName());
        requireExists("/_component_template/" + properties.getSettingsComponentTemplateName());
        requireExists("/_ilm/policy/" + properties.getDefaultLifecyclePolicyName());
        requireExists("/_ilm/policy/" + properties.getShortLifecyclePolicyName());
        requireExists("/_ilm/policy/" + properties.getErrorLifecyclePolicyName());
        requireExists("/_ilm/policy/" + properties.getAuditLifecyclePolicyName());
        requireExists("/_ilm/policy/" + properties.getAggregateLifecyclePolicyName());
        requireExists("/_index_template/" + properties.getIndexTemplateName());
        requireExists("/_index_template/" + properties.getShortIndexTemplateName());
        requireExists("/_index_template/" + properties.getErrorIndexTemplateName());
        requireExists("/_index_template/" + properties.getAuditIndexTemplateName());
        requireExists("/_index_template/" + properties.getAggregateIndexTemplateName());
    }

    /**
     * 获取模板和生命周期资源状态。
     *
     * @return 资源状态
     */
    public Map<String, Boolean> status() {
        Map<String, Boolean> status = new LinkedHashMap<>();
        status.put("componentTemplate." + properties.getEcsComponentTemplateName(),
                exists("/_component_template/" + properties.getEcsComponentTemplateName()));
        status.put("componentTemplate." + properties.getSettingsComponentTemplateName(),
                exists("/_component_template/" + properties.getSettingsComponentTemplateName()));
        status.put("lifecycle." + properties.getDefaultLifecyclePolicyName(),
                exists("/_ilm/policy/" + properties.getDefaultLifecyclePolicyName()));
        status.put("lifecycle." + properties.getShortLifecyclePolicyName(),
                exists("/_ilm/policy/" + properties.getShortLifecyclePolicyName()));
        status.put("lifecycle." + properties.getErrorLifecyclePolicyName(),
                exists("/_ilm/policy/" + properties.getErrorLifecyclePolicyName()));
        status.put("lifecycle." + properties.getAuditLifecyclePolicyName(),
                exists("/_ilm/policy/" + properties.getAuditLifecyclePolicyName()));
        status.put("lifecycle." + properties.getAggregateLifecyclePolicyName(),
                exists("/_ilm/policy/" + properties.getAggregateLifecyclePolicyName()));
        status.put("indexTemplate." + properties.getIndexTemplateName(),
                exists("/_index_template/" + properties.getIndexTemplateName()));
        status.put("indexTemplate." + properties.getShortIndexTemplateName(),
                exists("/_index_template/" + properties.getShortIndexTemplateName()));
        status.put("indexTemplate." + properties.getErrorIndexTemplateName(),
                exists("/_index_template/" + properties.getErrorIndexTemplateName()));
        status.put("indexTemplate." + properties.getAuditIndexTemplateName(),
                exists("/_index_template/" + properties.getAuditIndexTemplateName()));
        status.put("indexTemplate." + properties.getAggregateIndexTemplateName(),
                exists("/_index_template/" + properties.getAggregateIndexTemplateName()));
        return status;
    }

    /**
     * 模拟 data stream 名称匹配的 index template。
     *
     * @param dataStreamName data stream 名称
     * @return 模拟结果 JSON
     */
    public String simulateIndex(String dataStreamName) {
        Request request = new Request("POST", "/_index_template/_simulate_index/" + dataStreamName);
        try {
            Response response = restClient.performRequest(request);
            return response.getEntity() == null
                    ? "{}"
                    : new String(
                            response.getEntity().getContent().readAllBytes(),
                            StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to simulate Elaticsearch index template: " + dataStreamName, exception);
        }
    }

    private void putIfNeeded(String endpoint, String resourcePath, boolean overwrite) {
        if (!overwrite && exists(endpoint)) {
            return;
        }
        put(endpoint, resourcePath);
    }

    private void put(String endpoint, String resourcePath) {
        Request request = new Request("PUT", endpoint);
        request.setEntity(jsonEntity(resourcePath));
        try {
            restClient.performRequest(request);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to put Elaticsearch resource: " + endpoint, exception);
        }
    }

    private boolean exists(String endpoint) {
        Request request = new Request("HEAD", endpoint);
        try {
            restClient.performRequest(request);
            return true;
        } catch (ResponseException exception) {
            return exception.getResponse().getStatusLine().getStatusCode() != 404;
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to check Elaticsearch resource: " + endpoint, exception);
        }
    }

    private void requireExists(String endpoint) {
        if (!exists(endpoint)) {
            throw new IllegalStateException("Required Elaticsearch resource is missing: " + endpoint);
        }
    }

    private HttpEntity jsonEntity(String resourcePath) {
        try {
            ClassPathResource resource = new ClassPathResource(resourcePath);
            String json = resource.getContentAsString(StandardCharsets.UTF_8);
            return new StringEntity(json, ContentType.APPLICATION_JSON);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read Elaticsearch resource: " + resourcePath, exception);
        }
    }
}
