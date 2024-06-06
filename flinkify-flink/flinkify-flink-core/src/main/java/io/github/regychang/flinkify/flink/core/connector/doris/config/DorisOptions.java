package io.github.regychang.flinkify.flink.core.connector.doris.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;

import org.apache.flink.table.types.DataType;

import java.util.List;

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

    ConfigOption<String> DATABASE = ConfigOptions.key("database")
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
    ConfigOption<String> LABEL_PREFIX = ConfigOptions.key("label-prefix")
            .stringType()
            .defaultValue("")
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

    ConfigOption<String> READ_FIELDS = ConfigOptions.key("read-fields")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> FILTER_QUERY = ConfigOptions.key("filter-query")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_TABLET_SIZE = ConfigOptions.key("request-tablet-size")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_CONNECT_TIMEOUT_MS = ConfigOptions.key("request-connect-timeout-ms")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_READ_TIMEOUT_MS = ConfigOptions.key("request-read-timeout-ms")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_QUERY_TIMEOUT_S = ConfigOptions.key("request-query-timeout-s")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_RETRIES = ConfigOptions.key("request-retries")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> REQUEST_BATCH_SIZE = ConfigOptions.key("request-batch-size")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Long> EXEC_MEM_LIMIT = ConfigOptions.key("exec-mem-limit")
            .longType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Integer> DESERIALIZE_QUEUE_SIZE = ConfigOptions.key("deserialize-queue-size")
            .intType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Boolean> DESERIALIZE_ARROW_ASYNC = ConfigOptions.key("deserialize-arrow-async")
            .booleanType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Boolean> USE_OLD_API = ConfigOptions.key("use-old-api")
            .booleanType()
            .defaultValue(false)
            .withDescription("");
}
