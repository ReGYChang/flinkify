package io.github.regychang.flinkify.common.exception;

public class FlinkException extends QuantalinkException {

    public FlinkException(String errorCode, String message) {
        super(errorCode, message);
    }

    public FlinkException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
