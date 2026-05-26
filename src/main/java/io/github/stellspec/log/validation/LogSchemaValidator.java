package io.github.stellspec.log.validation;

import io.github.stellspec.log.domain.EcsLogDocument;
import org.springframework.stereotype.Component;

/** ECS 日志写入前 schema 校验器。 */
@Component
public class LogSchemaValidator {

    /**
     * 校验 data stream 写入必需字段。
     *
     * @param document ECS 日志文档
     */
    public void validate(EcsLogDocument document) {
        if (document == null) {
            throw new LogSchemaValidationException("log document is null");
        }
        require(document.getTimestamp() != null, "@timestamp is required");
        require(hasText(document.getDataStreamName()), "dataStreamName is required");
        require(document.getEvent() != null, "event is required");
        require(hasText(document.getEvent().dataset()), "event.dataset is required");
        require(hasText(document.getEvent().kind()), "event.kind is required");
        require(document.getService() != null, "service is required");
        require(hasText(document.getService().name()), "service.name is required");
        require(document.getStellflowSource() != null, "stellflow source is required");
    }

    private void require(boolean expression, String message) {
        if (!expression) {
            throw new LogSchemaValidationException(message);
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
