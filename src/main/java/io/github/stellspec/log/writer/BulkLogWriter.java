package io.github.stellspec.log.writer;

import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import java.util.List;

/** Bulk 日志写入器。 */
public interface BulkLogWriter {

    /**
     * 批量写入日志文档。
     *
     * @param documents 已路由文档
     * @return Bulk 写入结果
     */
    BulkWriteResult write(List<RoutedLogDocument> documents);
}
