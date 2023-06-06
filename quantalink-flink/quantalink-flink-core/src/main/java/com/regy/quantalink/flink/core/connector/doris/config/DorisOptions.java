package com.regy.quantalink.flink.core.connector.doris.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import org.apache.flink.table.types.DataType;

import java.util.List;

/**
 * @author regy
 */
public interface DorisOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("doris")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("The connector list of Doris.");

    ConfigOption<String> FE_NODES = ConfigOptions.key("fe-nodes")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> TABLE = ConfigOptions.key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("");
    ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("");
    ConfigOption<String> LABEL = ConfigOptions.key("label")
            .stringType()
            .noDefaultValue()
            .withDescription("");
    ConfigOption<List<String>> FIELDS = ConfigOptions.key("fields")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("");
    ConfigOption<List<DataType>> TYPES = ConfigOptions.key("types")
            .dataType()
            .asList()
            .noDefaultValue()
            .withDescription("");

}
