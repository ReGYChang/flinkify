package io.github.regychang.flinkify.flink.core.connector.telegram.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.connector.telegram.enums.TelegramMethod;
import io.github.regychang.flinkify.flink.core.connector.telegram.enums.HttpMethod;
import io.github.regychang.flinkify.flink.core.connector.telegram.enums.ParseMode;
import io.github.regychang.flinkify.flink.core.connector.telegram.enums.SinkRequestMode;

import java.util.List;

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
