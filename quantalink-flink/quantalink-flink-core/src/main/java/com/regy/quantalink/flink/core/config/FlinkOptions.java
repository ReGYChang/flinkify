package com.regy.quantalink.flink.core.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * @author regy
 */
public interface FlinkOptions {

    ConfigOption<String> JOB_NAME = ConfigOptions.key("flink.job-name")
            .stringType()
            .defaultValue(String.format("Flink Job - %s", LocalDateTime.now()))
            .withDescription("");

    ConfigOption<List<Configuration>> SOURCE_CONNECTORS = ConfigOptions.key("flink.sources")
            .configType()
            .asList()
            .defaultValues(new ArrayList<>())
            .withDescription("A list of source connector configurations.");

    ConfigOption<List<Configuration>> SINK_CONNECTORS = ConfigOptions.key("flink.sinks")
            .configType()
            .asList()
            .defaultValues(new ArrayList<>())
            .withDescription("A list of sink connector configurations.");
}
