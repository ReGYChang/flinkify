package com.regy.quantalink.flink.core.connector.nebula.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.nebula.enums.NebulaRowType;

import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;

import java.util.List;

/**
 * @author regy
 */
public interface NebulaOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("connectors")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> NEBULA_GRAPH_ADDRESS = ConfigOptions.key("graph-address")
            .stringType()
            .noDefaultValue()
            .withDescription("The address of the Nebula Graph service.");

    ConfigOption<String> NEBULA_META_ADDRESS = ConfigOptions.key("meta-address")
            .stringType()
            .noDefaultValue()
            .withDescription("The address of the Nebula Graph metadata service.");

    ConfigOption<String> NEBULA_USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("The username to connect to Nebula Graph.");

    ConfigOption<String> NEBULA_PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("The password to connect to Nebula Graph.");

    ConfigOption<WriteModeEnum> NEBULA_WRITE_MODE = ConfigOptions.key("write-mode")
            .enumType(WriteModeEnum.class)
            .defaultValue(WriteModeEnum.INSERT)
            .withDescription(
                    Description.builder()
                            .text("The write mode to use for Nebula Graph. Valid options are: ")
                            .list(
                                    TextElement.text("insert"),
                                    TextElement.text("update"),
                                    TextElement.text("delete"))
                            .build());

    ConfigOption<Integer> NEBULA_BATCH_SIZE = ConfigOptions.key("batch-size")
            .intType()
            .defaultValue(100)
            .withDescription("The number of records to buffer before writing them to Nebula Graph in a single batch.");

    ConfigOption<Integer> NEBULA_BATCH_INTERVAL_MS = ConfigOptions.key("batch-interval-ms")
            .intType()
            .defaultValue(500)
            .withDescription(
                    "The time interval in milliseconds to wait before writing records to Nebula Graph if the batch"
                            + " size has not been reached.");

    ConfigOption<String> NEBULA_GRAPH_SPACE = ConfigOptions.key("graph-space")
            .stringType()
            .noDefaultValue()
            .withDescription("The graph space name in Nebula Graph.");

    ConfigOption<NebulaRowType> NEBULA_ROW_TYPE = ConfigOptions.key("row-type")
            .enumType(NebulaRowType.class)
            .noDefaultValue()
            .withDescription(
                    Description.builder()
                            .text("The row type of the data. Valid options are: ")
                            .list(TextElement.text("Vertex"), TextElement.text("Edge"))
                            .build());

    ConfigOption<String> NEBULA_VERTEX_NAME = ConfigOptions.key("vertex-name")
            .stringType()
            .noDefaultValue()
            .withDescription("The vertex tag name in Nebula Graph.");

    ConfigOption<Integer> NEBULA_VERTEX_ID_INDEX = ConfigOptions.key("vertex-id-index")
            .intType()
            .noDefaultValue()
            .withDescription("The position index of the vertex ID in the input data.");

    ConfigOption<List<String>> NEBULA_VERTEX_FIELDS = ConfigOptions.key("vertex-fields")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("The list of vertex property names in the input data.");

    ConfigOption<List<Integer>> NEBULA_VERTEX_POSITIONS = ConfigOptions.key("vertex-positions")
            .intType()
            .asList()
            .noDefaultValue()
            .withDescription("The list of the positions of the vertex properties in the input data.");

    ConfigOption<String> NEBULA_EDGE_NAME = ConfigOptions.key("edge-name")
            .stringType()
            .noDefaultValue()
            .withDescription("The edge type name in Nebula Graph.");

    ConfigOption<Integer> NEBULA_EDGE_SRC_INDEX = ConfigOptions.key("edge-src-index")
            .intType()
            .noDefaultValue()
            .withDescription("The index of the edge source vertex ID in the input data.");

    ConfigOption<Integer> NEBULA_EDGE_DST_INDEX = ConfigOptions.key("edge-dst-index")
            .intType()
            .noDefaultValue()
            .withDescription("The index of the edge destination vertex ID in the input data.");

    ConfigOption<Integer> NEBULA_EDGE_RANK_INDEX = ConfigOptions.key("edge-rank-index")
            .intType()
            .defaultValue(-1)
            .withDescription(
                    "The index of the edge rank in the input data. Edge rank is optional. "
                            + "It specifies the edge rank of the same edge type. If not specified, the default value is -1.");

    ConfigOption<List<String>> NEBULA_EDGE_FIELDS = ConfigOptions.key("edge-fields")
            .stringType()
            .asList()
            .noDefaultValue()
            .withDescription("The list of edge property names in the input data.");

    ConfigOption<List<Integer>> NEBULA_EDGE_POSITIONS = ConfigOptions.key("edge-positions")
            .intType()
            .asList()
            .noDefaultValue()
            .withDescription("The positions of the edge properties in the input data.");
}
