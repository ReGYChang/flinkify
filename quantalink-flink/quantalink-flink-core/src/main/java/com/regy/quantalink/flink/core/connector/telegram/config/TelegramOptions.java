package com.regy.quantalink.flink.core.connector.telegram.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.telegram.enums.TelegramMethod;
import com.regy.quantalink.flink.core.connector.telegram.enums.HttpMethod;
import com.regy.quantalink.flink.core.connector.telegram.enums.ParseMode;
import com.regy.quantalink.flink.core.connector.telegram.enums.SinkRequestMode;

import java.util.List;

/**
 * @author regy
 */
public interface TelegramOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("telegram")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Boolean> ALLOW_SELF_SIGNED = ConfigOptions.key("allow-self-signed")
            .booleanType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<SinkRequestMode> SINK_REQUEST_MODE = ConfigOptions.key("sink-request-mode")
            .enumType(SinkRequestMode.class)
            .defaultValue(SinkRequestMode.SINGLE)
            .withDescription("");

    ConfigOption<Configuration> HEADER = ConfigOptions.key("header")
            .configType()
            .defaultValue(new Configuration())
            .withDescription("");

    ConfigOption<HttpMethod> HTTP_METHOD = ConfigOptions.key("http-method")
            .enumType(HttpMethod.class)
            .defaultValue(HttpMethod.GET)
            .withDescription("");

    ConfigOption<String> TOKEN = ConfigOptions.key("token")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> CHAT_ID = ConfigOptions.key("chat-id")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<ParseMode> PARSE_MODE = ConfigOptions.key("parse-mode")
            .enumType(ParseMode.class)
            .defaultValue(ParseMode.TEXT)
            .withDescription("");

    ConfigOption<TelegramMethod> BOT_METHOD = ConfigOptions.key("telegram-method")
            .enumType(TelegramMethod.class)
            .noDefaultValue()
            .withDescription("");
}
