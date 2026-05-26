package io.github.stellspec.log.writer;

import io.github.stellspec.config.StellspecLogProperties;
import io.github.stellspec.log.domain.BulkWriteResult;
import io.github.stellspec.log.domain.RoutedLogDocument;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Bulk 写入缓冲区。 */
@Component
@RequiredArgsConstructor
public class BulkLogBuffer {

    private final StellspecLogProperties properties;

    private final BulkLogWriter writer;

    private final List<RoutedLogDocument> pending = new ArrayList<>();

    /**
     * 添加文档并在达到阈值时 flush。
     *
     * @param document 已路由文档
     * @return 可选写入结果
     */
    public synchronized BulkWriteResult add(RoutedLogDocument document) {
        pending.add(document);
        if (pending.size() >= properties.getBulk().getMaxActions()) {
            return flush();
        }
        return BulkWriteResult.success(0);
    }

    /**
     * 立即批量写入一组文档。
     *
     * @param documents 已路由文档
     * @return Bulk 写入结果
     */
    public synchronized BulkWriteResult writeNow(List<RoutedLogDocument> documents) {
        return writer.write(documents);
    }

    /**
     * Flush 当前缓冲区。
     *
     * @return Bulk 写入结果
     */
    public synchronized BulkWriteResult flush() {
        if (pending.isEmpty()) {
            return BulkWriteResult.success(0);
        }
        List<RoutedLogDocument> snapshot = new ArrayList<>(pending);
        pending.clear();
        return writer.write(snapshot);
    }

    /**
     * 获取缓冲区大小。
     *
     * @return 待写入文档数
     */
    public synchronized int size() {
        return pending.size();
    }

    /** 应用关闭时 flush 未写入文档。 */
    @PreDestroy
    public void close() {
        flush();
    }
}
