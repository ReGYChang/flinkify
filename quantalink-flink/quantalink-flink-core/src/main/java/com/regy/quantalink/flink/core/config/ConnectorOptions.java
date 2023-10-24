package com.regy.quantalink.flink.core.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;

/**
 * @author regy
 */
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
