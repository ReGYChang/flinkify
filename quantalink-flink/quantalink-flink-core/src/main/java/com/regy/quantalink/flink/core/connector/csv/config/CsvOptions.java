package com.regy.quantalink.flink.core.connector.csv.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import java.util.List;

/**
 * @author regy
 */
public interface CsvOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("csv")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("The connector list of CSV.");

    ConfigOption<String> FILE_PATH = ConfigOptions.key("file-path")
            .stringType()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<Boolean> USES_HEADER = ConfigOptions.key("uses-header")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Boolean> REORDER_COLUMNS = ConfigOptions.key("reorder-columns")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Boolean> STRICT_HEADERS = ConfigOptions.key("strict-headers")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Boolean> SKIP_FIRST_DATA_ROW = ConfigOptions.key("skip-first-data-row")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Boolean> ALLOW_COMMENTS = ConfigOptions.key("allow-comments")
            .booleanType()
            .defaultValue(false)
            .withDescription("");

    ConfigOption<Character> COLUMN_SEPARATOR = ConfigOptions.key("column-separator")
            .characterType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<String> ARRAY_ELEMENT_SEPARATOR = ConfigOptions.key("array-element-separator")
            .stringType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<Character> QUOTE_CHAR = ConfigOptions.key("quote-char")
            .characterType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<Character> ESCAPE_CHAR = ConfigOptions.key("escape-char")
            .characterType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<String> LINE_SEPARATOR = ConfigOptions.key("line-separator")
            .stringType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<String> NULL_VALUE = ConfigOptions.key("null-value")
            .stringType()
            .defaultValue(null)
            .withDescription("");

    ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors")
            .booleanType()
            .defaultValue(false)
            .withDescription("");
}
