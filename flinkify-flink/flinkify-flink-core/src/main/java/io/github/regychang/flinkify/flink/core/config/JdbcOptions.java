package io.github.regychang.flinkify.flink.core.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;

public interface JdbcOptions {

    ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of the database server.");

    ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Port number of the database server.");

    ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to connect to the database.");

    ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to connect to the database.");

    ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the database to connect to.");

    ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("Schema name in the database.");

    ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name in the database.");

    ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("Number of records to batch before sending to the database.");

    ConfigOption<Long> BATCH_INTERVAL_MS =
            ConfigOptions.key("batch-interval-ms")
                    .longType()
                    .defaultValue(200L)
                    .withDescription("Interval in milliseconds between batch sends.");

    ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries for failed operations.");
}
