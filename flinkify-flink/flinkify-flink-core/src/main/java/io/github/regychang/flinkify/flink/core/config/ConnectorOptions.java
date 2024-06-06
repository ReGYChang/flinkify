package io.github.regychang.flinkify.flink.core.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;

public interface ConnectorOptions {

    ConfigOption<String> ID = ConfigOptions.key("id")
            .stringType()
            .defaultValue("Undefined")
            .withDescription("The name of the connector.");

    ConfigOption<String> NAME = ConfigOptions.key("name")
            .stringType()
            .defaultValue("Undefined")
            .withDescription("The name of the connector.");

    ConfigOption<Integer> PARALLELISM = ConfigOptions.key("parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("The level of parallelism for the connector operator.");
}
