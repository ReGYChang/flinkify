package com.regy.quantalink.flink.core.connector.datagen.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

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
            .noDefaultValue()
            .withDescription("");
}
