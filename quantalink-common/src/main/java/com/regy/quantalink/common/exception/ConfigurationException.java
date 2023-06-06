package com.regy.quantalink.common.exception;

/**
 * @author regy
 */
public class ConfigurationException extends QuantalinkException {

    public ConfigurationException(String errorCode, String message) {
        super(errorCode, message);
    }

    public ConfigurationException(String errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
