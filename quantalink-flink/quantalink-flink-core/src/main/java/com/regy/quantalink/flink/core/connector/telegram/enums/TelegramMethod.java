package com.regy.quantalink.flink.core.connector.telegram.enums;

import lombok.Getter;

/**
 * @author regy
 */

@Getter
public enum TelegramMethod {
    GET_ME("getMe"),
    GET_CHAT("getChat"),
    SEND_MESSAGE("sendMessage");

    private String method;

    TelegramMethod(String method) {
        this.method = method;
    }
}
