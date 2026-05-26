package io.github.stellspec.config;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** StellSpec Elaticsearch 治理配置。 */
@Getter
@Setter
@ConfigurationProperties(prefix = "stellspec.elaticsearch")
public class StellspecElaticsearchProperties {

    /** 是否启用 template 和 lifecycle bootstrap。 */
    private boolean bootstrapEnabled = false;

    /** Bootstrap 模式。 */
    private BootstrapMode bootstrapMode = BootstrapMode.CREATE_IF_ABSENT;

    /** 是否覆盖已有模板。 */
    private boolean overwriteTemplates = false;

    /** Component template 名称。 */
    private String ecsComponentTemplateName = "stellspec-ecs-mappings";

    /** Settings component template 名称。 */
    private String settingsComponentTemplateName = "stellspec-log-settings";

    /** Index template 名称。 */
    private String indexTemplateName = "stellspec-logs-template";

    /** 短保留 index template 名称。 */
    private String shortIndexTemplateName = "stellspec-logs-short-template";

    /** 错误日志 index template 名称。 */
    private String errorIndexTemplateName = "stellspec-logs-error-template";

    /** 审计日志 index template 名称。 */
    private String auditIndexTemplateName = "stellspec-logs-audit-template";

    /** 聚合日志 index template 名称。 */
    private String aggregateIndexTemplateName = "stellspec-logs-aggregate-template";

    /** 默认 lifecycle policy 名称。 */
    private String defaultLifecyclePolicyName = "stellspec-logs-default-policy";

    /** 短保留 lifecycle policy 名称。 */
    private String shortLifecyclePolicyName = "stellspec-logs-short-policy";

    /** 错误日志 lifecycle policy 名称。 */
    private String errorLifecyclePolicyName = "stellspec-logs-error-policy";

    /** 审计日志 lifecycle policy 名称。 */
    private String auditLifecyclePolicyName = "stellspec-logs-audit-policy";

    /** 聚合日志 lifecycle policy 名称。 */
    private String aggregateLifecyclePolicyName = "stellspec-logs-aggregate-policy";

    /** 匹配的 data stream pattern。 */
    private String indexPattern = "logs-*-*";

    /** 短保留 data stream pattern。 */
    private List<String> shortIndexPatterns = new ArrayList<>(List.of("logs-*-access-*", "logs-*-debug-*"));

    /** 错误日志 data stream pattern。 */
    private List<String> errorIndexPatterns = new ArrayList<>(List.of("logs-*-error-*"));

    /** 审计日志 data stream pattern。 */
    private List<String> auditIndexPatterns = new ArrayList<>(List.of("logs-audit-*"));

    /** 聚合日志 data stream pattern。 */
    private List<String> aggregateIndexPatterns = new ArrayList<>(List.of("logs-stellspec-aggregate-*"));

    /** Bootstrap 模式。 */
    public enum BootstrapMode {
        DISABLED,
        VALIDATE_ONLY,
        CREATE_IF_ABSENT,
        OVERWRITE
    }
}
