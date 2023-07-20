package com.regy.quantalink.flink.core.connector.telegram.sink;

import com.alibaba.fastjson2.annotation.JSONField;

/**
 * @author regy
 */
public class TelegramPayload {

    @JSONField(name = "text")
    public String text;

    @JSONField(name = "chat_id")
    public String chatId;

    @JSONField(name = "parse_mode")
    public String parseMode;


    public void setChatId(String chatId) {
        this.chatId = chatId;
    }

    public void setParseMode(String parseMode) {
        this.parseMode = parseMode;
    }

    public TelegramPayload() {
    }

    public TelegramPayload(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "TelegramPayload{" +
                "text='" + text + '\'' +
                ", chatId='" + chatId + '\'' +
                ", parseMode='" + parseMode + '\'' +
                '}';
    }
}
