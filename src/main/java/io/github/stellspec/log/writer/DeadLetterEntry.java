package io.github.stellspec.log.writer;

import io.github.stellflux.stellflow.message.StellflowMessage;

/** Dead letter 写入条目。 */
public record DeadLetterEntry(
        StellflowMessage message,
        String stage,
        String reason,
        String failedDataStream,
        String failedDocumentId,
        Integer status,
        String errorType) {}
