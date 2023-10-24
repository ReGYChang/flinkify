package com.regy.quantalink.flink.core.connector.mysql.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import java.util.List;

/**
 * @author regy
 */
public interface MySqlOptions {

    ConfigOption<List<Configuration>> CDC = ConfigOptions.key("mysql-cdc")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> DATABASE_LIST = ConfigOptions.key("database-list")
            .stringType()
            .defaultValue(".*")
            .withDescription("");

    ConfigOption<String> TABLE_LIST = ConfigOptions.key("table-list")
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

    ConfigOption<String> SERVER_TIME_ZONE = ConfigOptions.key("server-time-zone")
            .stringType()
            .defaultValue("UTC")
            .withDescription("");

    ConfigOption<Boolean> INCLUDE_SCHEMA = ConfigOptions.key("include-schema")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Boolean> INCLUDE_SCHEMA_CHANGES = ConfigOptions.key("include-schema-changes")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

}
