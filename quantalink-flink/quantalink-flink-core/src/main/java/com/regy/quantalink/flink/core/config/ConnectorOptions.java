package com.regy.quantalink.flink.core.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;

/**
 * @author regy
 */
public interface ConnectorOptions {

    ConfigOption<String> NAME = ConfigOptions.key("name")
            .stringType()
            .defaultValue("NoName")
            .withDescription("The name of the source connector.");

    ConfigOption<Integer> PARALLELISM = ConfigOptions.key("parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("The level of parallelism for the source operator.");
}
