package com.regy.quantalink.flink.core.connector.telegram.enums;

/**
 * @author regy
 */

public enum TelegramMethod {
    GET_ME("getMe"),
    SEND_MESSAGE("sendMessage");

    private String method;

    TelegramMethod(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }
}
