package io.github.regychang.flinkify.flink.core.connector.telegram.sink;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Setter;

public class TelegramPayload {

    @JSONField(name = "text")
    public String text;

    @Setter
    @JSONField(name = "chat_id")
    public String chatId;

    @Setter
    @JSONField(name = "parse_mode")
    public String parseMode;


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
