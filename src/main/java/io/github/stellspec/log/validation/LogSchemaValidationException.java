package io.github.stellspec.log.validation;

/** 日志 schema 校验异常。 */
public class LogSchemaValidationException extends RuntimeException {

    public LogSchemaValidationException(String message) {
        super(message);
    }
}
