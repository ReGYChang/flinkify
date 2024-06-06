package io.github.regychang.flinkify.flink.core.connector.telegram.enums;

import lombok.Getter;

@Getter
public enum TelegramMethod {

    GET_ME("getMe"),

    GET_CHAT("getChat"),

    SEND_MESSAGE("sendMessage");

    private final String method;

    TelegramMethod(String method) {
        this.method = method;
    }
}
