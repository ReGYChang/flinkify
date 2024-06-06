package io.github.regychang.flinkify.common.exception;

import lombok.Getter;

@Getter
public class QuantalinkException extends RuntimeException {
    protected final String errorCode;

    public QuantalinkException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public QuantalinkException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
}
