package io.github.stellspec.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.stellflux.stellflow.message.StellflowMessage;
import io.github.stellspec.log.domain.IngestionResult;
import io.github.stellspec.log.writer.BulkFailureHandler;
import io.github.stellspec.log.writer.BulkLogBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StellspecLogIngestionServiceTest {

    @Mock
    private LogProcessingPipeline pipeline;

    @Mock
    private BulkLogBuffer bulkLogBuffer;

    @Mock
    private BulkFailureHandler bulkFailureHandler;

    @Mock
    private ElaticsearchWriteProgressLogger writeProgressLogger;

    @Test
    void ingestShouldSkipWriteWhenPipelineReturnsNoDocuments() {
        StellspecLogIngestionService service =
                new StellspecLogIngestionService(pipeline, bulkLogBuffer, bulkFailureHandler, writeProgressLogger);
        StellflowMessage message = StellflowMessage.ofString("stellspec.logs", "key-1", "payload");
        when(pipeline.process(message)).thenReturn(List.of());

        IngestionResult result = service.ingest(message);

        assertThat(result.result()).isEqualTo("skipped");
        assertThat(result.id()).isNull();
        assertThat(result.indexName()).isNull();
        verify(bulkLogBuffer, never()).writeNow(List.of());
        verify(writeProgressLogger).recordSkipped(message);
    }
}
