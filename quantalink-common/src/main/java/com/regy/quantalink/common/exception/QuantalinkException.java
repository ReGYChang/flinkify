package com.regy.quantalink.common.exception;

/**
 * @author regy
 */
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

    public String getErrorCode() {
        return errorCode;
    }
}
