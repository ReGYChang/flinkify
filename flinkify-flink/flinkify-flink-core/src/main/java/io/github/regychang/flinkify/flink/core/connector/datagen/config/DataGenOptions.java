package io.github.regychang.flinkify.flink.core.connector.datagen.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;

import java.util.List;

public interface DataGenOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("datagen")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("The connector list of DataGen.");

    ConfigOption<Long> NUMBER_OF_ROWS = ConfigOptions.key("number-of-rows")
            .longType()
            .defaultValue(Long.MAX_VALUE)
            .withDescription("");

    ConfigOption<Long> ROWS_PER_SECOND = ConfigOptions.key("rows-per-second")
            .longType()
            .defaultValue(1000L)
            .withDescription("");
}
