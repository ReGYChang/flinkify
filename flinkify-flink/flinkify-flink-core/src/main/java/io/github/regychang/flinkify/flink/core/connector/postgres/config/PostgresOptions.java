package io.github.regychang.flinkify.flink.core.connector.postgres.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;

import com.ververica.cdc.connectors.base.options.StartupMode;

import java.util.List;

public interface PostgresOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("postgres")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("The connector list of Postgres.");

    ConfigOption<List<Configuration>> CDC = ConfigOptions.key("postgres-cdc")
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
            .defaultValue(5432)
            .withDescription("");

    ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> DATABASE = ConfigOptions.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> SCHEMA = ConfigOptions.key("schema")
            .stringType()
            .defaultValue("public")
            .withDescription("");

    ConfigOption<String> TABLE = ConfigOptions.key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<List<String>> SCHEMA_LIST = ConfigOptions.key("schema-list")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<List<String>> TABLE_LIST = ConfigOptions.key("table-list")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> SLOT_NAME = ConfigOptions.key("slot-name")
            .stringType()
            .defaultValue("flink")
            .withDescription("");

    ConfigOption<String> DECODING_PLUGIN_NAME = ConfigOptions.key("decoding-plugin-name")
            .stringType()
            .defaultValue("pgoutput")
            .withDescription("");

    ConfigOption<Boolean> INCLUDE_SCHEMA_CHANGE = ConfigOptions.key("include-schema-change")
            .booleanType()
            .defaultValue(true)
            .withDescription("");

    ConfigOption<Integer> SPLIT_SIZE = ConfigOptions.key("split-size")
            .intType()
            .defaultValue(2)
            .withDescription("");

    ConfigOption<StartupMode> STARTUP_MODE = ConfigOptions.key("startup-mode")
            .enumType(StartupMode.class)
            .defaultValue(StartupMode.INITIAL)
            .withDescription("");

    ConfigOption<Configuration> DEBEZIUM_PROPERTIES = ConfigOptions.key("debezium-properties")
            .configType()
            .defaultValue(new Configuration())
            .withDescription("");

    ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size")
            .intType()
            .defaultValue(5000)
            .withDescription("");

    ConfigOption<Long> BATCH_INTERVAL_MS = ConfigOptions.key("batch-interval-ms")
            .longType()
            .defaultValue(0L)
            .withDescription("");

    ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("");
}
