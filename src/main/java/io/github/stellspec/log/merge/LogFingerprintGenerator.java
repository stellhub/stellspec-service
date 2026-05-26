package io.github.stellspec.log.merge;

import io.github.stellspec.log.domain.EcsLogDocument;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.stereotype.Component;

/** 日志 fingerprint 生成器。 */
@Component
public class LogFingerprintGenerator {

    /**
     * 根据归一化日志内容生成 event.hash。
     *
     * @param document ECS 日志文档
     * @return fingerprint
     */
    public String generate(EcsLogDocument document) {
        String service = document.getService() == null ? "" : document.getService().name();
        String level = document.getLog() == null ? "" : document.getLog().level();
        String errorType = document.getError() == null ? "" : document.getError().type();
        String message = normalize(document.getMessage());
        String stackTop = normalize(stackTop(document));
        return sha256(service + "|" + level + "|" + errorType + "|" + message + "|" + stackTop);
    }

    private String stackTop(EcsLogDocument document) {
        if (document.getError() == null || document.getError().stackTrace() == null) {
            return "";
        }
        String[] lines = document.getError().stackTrace().split("\\R");
        return lines.length == 0 ? "" : lines[0];
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.replaceAll("[0-9a-fA-F]{8}-[0-9a-fA-F-]{27,}", "<uuid>")
                .replaceAll("\\b[0-9]{2,}\\b", "<num>")
                .replaceAll("\\b[0-9a-fA-F]{16,}\\b", "<hex>");
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 digest is not available", exception);
        }
    }
}
