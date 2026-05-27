package io.github.stellspec.log.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Elaticsearch source 字段值转换工具。 */
final class ElaticsearchSourceValues {

    private ElaticsearchSourceValues() {}

    /**
     * 转换为 JacksonJsonpMapper 可直接序列化的 source map。
     *
     * @param source 原始 source map
     * @return 可写入 Elaticsearch 的 source map
     */
    static Map<String, Object> toSourceMap(Map<String, Object> source) {
        Map<String, Object> converted = new LinkedHashMap<>();
        source.forEach((key, value) -> converted.put(key, toSourceValue(value)));
        return converted;
    }

    private static Object toSourceValue(Object value) {
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> converted = new LinkedHashMap<>();
            map.forEach((key, nestedValue) -> converted.put(String.valueOf(key), toSourceValue(nestedValue)));
            return converted;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> converted = new ArrayList<>();
            iterable.forEach(item -> converted.add(toSourceValue(item)));
            return converted;
        }
        return value;
    }
}
