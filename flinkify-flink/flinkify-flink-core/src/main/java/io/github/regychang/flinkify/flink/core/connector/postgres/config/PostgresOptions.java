package io.github.regychang.flinkify.flink.core.connector.postgres.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;

import org.apache.flink.cdc.connectors.base.options.StartupMode;

import java.util.List;

public interface PostgresOptions {

    ConfigOption<List<Configuration>> CONNECTORS =
            ConfigOptions.key("postgres")
                    .configType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The list of Postgres connectors to be used for data integration.");

    ConfigOption<List<Configuration>> CDC =
            ConfigOptions.key("postgres-cdc")
                    .configType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Configuration for Change Data Capture (CDC) using Postgres.");

    ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The hostname or IP address of the Postgres server.");

    ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("The port number on which the Postgres server is listening.");

    ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for connecting to the Postgres database.");

    ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for connecting to the Postgres database.");

    ConfigOption<String> DATABASE =
            ConfigOptions.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Postgres database to connect to.");

    ConfigOption<String> SCHEMA =
            ConfigOptions.key("schema")
                    .stringType()
                    .defaultValue("public")
                    .withDescription("The default schema to use within the Postgres database.");

    ConfigOption<String> TABLE =
            ConfigOptions.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the table to interact with in the Postgres database.");

    ConfigOption<List<String>> SCHEMA_LIST =
            ConfigOptions.key("schema-list")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("A list of schemas to include in the data processing.");

    ConfigOption<List<String>> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("A list of tables to include in the data processing.");

    ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot-name")
                    .stringType()
                    .defaultValue("flink")
                    .withDescription("The name of the replication slot used for CDC in Postgres.");

    ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding-plugin-name")
                    .stringType()
                    .defaultValue("pgoutput")
                    .withDescription("The name of the logical decoding plugin used for CDC.");

    ConfigOption<Integer> SPLIT_SIZE =
            ConfigOptions.key("split-size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription("The size of each data split for processing.");

    ConfigOption<StartupMode> STARTUP_MODE =
            ConfigOptions.key("startup-mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "The startup mode for the connector, determining how it initializes.");

    ConfigOption<Configuration> DEBEZIUM_PROPERTIES =
            ConfigOptions.key("debezium-properties")
                    .configType()
                    .defaultValue(new Configuration())
                    .withDescription(
                            "Additional properties for configuring Debezium, the CDC engine.");
}
