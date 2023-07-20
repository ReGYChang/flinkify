package com.regy.quantalink.flink.core.connector.telegram.func;

import com.regy.quantalink.flink.core.connector.telegram.sink.TelegramPayload;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

/**
 * @author regy
 */
public class SetupPayloadFunc implements MapFunction<TelegramPayload, TelegramPayload>, Serializable {

    private final String parseMode;
    private final String chatId;

    public SetupPayloadFunc(String parseMode, String chatId) {
        this.parseMode = parseMode;
        this.chatId = chatId;
    }

    @Override
    public TelegramPayload map(TelegramPayload payload) throws Exception {
        payload.setParseMode(parseMode);
        payload.setChatId(chatId);

        return payload;
    }
}
