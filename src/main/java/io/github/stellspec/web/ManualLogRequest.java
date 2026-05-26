package io.github.stellspec.web;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

/** 手工日志写入请求。 */
@Getter
@Setter
public class ManualLogRequest {

    /** 可选消息主题。 */
    private String topic;

    /** 可选消息键。 */
    private String key;

    /** 原始日志内容，支持 JSON 或普通文本。 */
    @NotBlank
    private String payload;
}
