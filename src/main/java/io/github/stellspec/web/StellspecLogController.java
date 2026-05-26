package io.github.stellspec.web;

import io.github.stellflux.elaticsearch.StellfluxElaticsearchProperties;
import io.github.stellflux.stellflow.StellfluxStellflowProperties;
import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.config.StellspecElaticsearchProperties;
import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.StellspecLogIngestionService;
import io.github.stellspec.log.consumer.StellflowBulkConsumerWorker;
import io.github.stellspec.log.domain.IngestionResult;
import io.github.stellspec.log.replay.DeadLetterReplayRequest;
import io.github.stellspec.log.replay.DeadLetterReplayResponse;
import io.github.stellspec.log.replay.DeadLetterReplayService;
import io.github.stellspec.log.template.ElaticsearchTemplateManager;
import io.github.stellspec.log.writer.BulkFailureHandler;
import io.github.stellspec.log.writer.BulkLogBuffer;
import io.github.stellspec.log.writer.ElaticsearchBulkLogWriter;
import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** StellSpec 日志服务 HTTP 接口。 */
@RestController
@RequestMapping("/api/stellspec/logs")
@RequiredArgsConstructor
public class StellspecLogController {

    private static final String DEFAULT_TOPIC = "stellspec.logs";

    private final StellspecLogIngestionService ingestionService;

    private final StellspecLogProperties logProperties;

    private final StellspecElaticsearchProperties stellspecElaticsearchProperties;

    private final StellfluxStellflowProperties stellflowProperties;

    private final StellfluxElaticsearchProperties elaticsearchProperties;

    private final BulkLogBuffer bulkLogBuffer;

    private final ElaticsearchBulkLogWriter bulkLogWriter;

    private final BulkFailureHandler bulkFailureHandler;

    private final StellflowBulkConsumerWorker bulkConsumerWorker;

    private final ElaticsearchTemplateManager templateManager;

    private final DeadLetterReplayService deadLetterReplayService;

    /**
     * 获取日志摄取服务状态。
     *
     * @return 服务状态
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("module", "stellspec-service");
        status.put("indexPrefix", logProperties.getIndexPrefix());
        status.put("dataStreamPattern", stellspecElaticsearchProperties.getIndexPattern());
        status.put("namespace", logProperties.getNamespaceDefault());
        status.put("bulkPending", bulkLogBuffer.size());
        status.put("bulkFlushCount", bulkLogWriter.getFlushCount().get());
        status.put("bulkSuccessCount", bulkLogWriter.getSuccessCount().get());
        status.put("bulkFailureCount", bulkLogWriter.getFailureCount().get());
        status.put("bulkRetryAttemptCount", bulkFailureHandler.getRetryAttemptCount().get());
        status.put("bulkDeadLetterCount", bulkFailureHandler.getDeadLetterCount().get());
        status.put("stellflowBootstrapServers", stellflowProperties.getBootstrapServers());
        status.put("consumerTopics", consumerTopics());
        status.put("consumerGroupId", stellflowProperties.getConsumer().getGroupId());
        status.put("elaticsearchEndpoints", elaticsearchProperties.getEndpoints());
        status.put("message", "Stellflow listener and Elaticsearch writer are initialized");
        return status;
    }

    /**
     * 获取 Bulk 写入与失败处理状态。
     *
     * @return Bulk 状态
     */
    @GetMapping("/bulk/status")
    public BulkStatusResponse bulkStatus() {
        return new BulkStatusResponse(
                bulkLogBuffer.size(),
                bulkLogWriter.getFlushCount().get(),
                bulkLogWriter.getSuccessCount().get(),
                bulkLogWriter.getFailureCount().get(),
                bulkFailureHandler.getRetryAttemptCount().get(),
                bulkFailureHandler.getRetryDocumentCount().get(),
                bulkFailureHandler.getRetrySuccessCount().get(),
                bulkFailureHandler.getDeadLetterCount().get(),
                bulkFailureHandler.getDeadLetterFailureCount().get(),
                bulkFailureHandler.getUnresolvedFailureCount().get(),
                bulkFailureHandler.getLastFailureAt(),
                bulkFailureHandler.getLastFailureSummary());
    }

    /**
     * 获取 Stellflow consumer 状态。
     *
     * @return consumer 状态
     */
    @GetMapping("/consumer/status")
    public ConsumerStatusResponse consumerStatus() {
        Map<String, Long> committedOffsets = new LinkedHashMap<>();
        bulkConsumerWorker.getCommittedOffsets()
                .forEach((key, value) -> committedOffsets.put(key.topic() + ":" + key.partition(), value));
        return new ConsumerStatusResponse(
                logProperties.getBulkConsumer().isEnabled(),
                bulkConsumerWorker.isRunning(),
                bulkConsumerWorker.getInFlightRecords().get(),
                bulkConsumerWorker.getInFlightRecords().get(),
                bulkConsumerWorker.getPollCount().get(),
                bulkConsumerWorker.getPolledRecordCount().get(),
                bulkConsumerWorker.getProcessedRecordCount().get(),
                bulkConsumerWorker.getFilteredRecordCount().get(),
                bulkConsumerWorker.getCommittedOffsetCount().get(),
                bulkConsumerWorker.getCommitFailureCount().get(),
                bulkConsumerWorker.getBackpressurePauseCount().get(),
                bulkConsumerWorker.getLastPollAt(),
                bulkConsumerWorker.getLastCommitAt(),
                bulkConsumerWorker.getLastFailureAt(),
                bulkConsumerWorker.getLastError(),
                committedOffsets);
    }

    /**
     * 获取 Elaticsearch template 与 lifecycle 状态。
     *
     * @return template 状态
     */
    @GetMapping("/templates/status")
    public Map<String, Boolean> templateStatus() {
        return templateManager.status();
    }

    /**
     * 模拟 data stream 匹配的 index template。
     *
     * @param request simulation 请求
     * @return simulation 结果
     */
    @PostMapping(
            value = "/templates/simulate",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public TemplateSimulationResponse simulateTemplate(@Valid @RequestBody TemplateSimulationRequest request) {
        return new TemplateSimulationResponse(
                request.dataStreamName(),
                templateManager.simulateIndex(request.dataStreamName()));
    }

    /**
     * 进程存活检查。
     *
     * @return 存活状态
     */
    @GetMapping("/health/liveness")
    public HealthStatusResponse liveness() {
        return new HealthStatusResponse("UP", Map.of("module", "stellspec-service"));
    }

    /**
     * 服务就绪检查。
     *
     * @return 就绪状态
     */
    @GetMapping("/health/readiness")
    public HealthStatusResponse readiness() {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("bulkConsumerEnabled", logProperties.getBulkConsumer().isEnabled());
        details.put("bulkConsumerRunning", bulkConsumerWorker.isRunning());
        try {
            Map<String, Boolean> templates = templateManager.status();
            details.put("templates", templates);
            boolean templatesReady = templates.values().stream().allMatch(Boolean::booleanValue);
            boolean consumerReady = !logProperties.getBulkConsumer().isEnabled() || bulkConsumerWorker.isRunning();
            return new HealthStatusResponse(templatesReady && consumerReady ? "UP" : "DOWN", details);
        } catch (RuntimeException exception) {
            details.put("templateError", exception.getMessage());
            return new HealthStatusResponse("DOWN", details);
        }
    }

    /**
     * 重放 dead letter 日志。
     *
     * @param request replay 请求
     * @return replay 结果
     */
    @PostMapping(
            value = "/dead-letter/replay",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public DeadLetterReplayResponse replayDeadLetter(@RequestBody DeadLetterReplayRequest request) {
        return deadLetterReplayService.replay(request);
    }

    /**
     * 手工写入一条日志，便于本地验证归一化和 Elaticsearch 写入链路。
     *
     * @param request 手工日志请求
     * @return 写入结果
     */
    @PostMapping(
            value = "/manual",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public IngestionResult ingestManual(@Valid @RequestBody ManualLogRequest request) {
        String topic = hasText(request.getTopic()) ? request.getTopic() : firstConsumerTopic();
        StellflowMessage message = StellflowMessage.ofString(topic, request.getKey(), request.getPayload());
        return ingestionService.ingest(message);
    }

    private List<String> consumerTopics() {
        List<String> topics = stellflowProperties.getConsumer().effectiveTopics();
        return topics.isEmpty() ? List.of(DEFAULT_TOPIC) : topics;
    }

    private String firstConsumerTopic() {
        return consumerTopics().getFirst();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
