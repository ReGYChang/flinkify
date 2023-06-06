package com.regy.quantalink.flink.core.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.ConnectorType;

import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

/**
 * @author regy
 */
public interface ConnectorOptions {

    ConfigOption<ConnectorType> CONNECTOR_TYPE = ConfigOptions.key("connector-type")
            .enumType(ConnectorType.class)
            .noDefaultValue()
            .withDescription(
                    Description.builder()
                            .text("The type of source connector to be used for data extraction. Supported source connector type: ")
                            .list(TextElement.text("kafka"), TextElement.text("rabbitmq"))
                            .build());

    ConfigOption<TypeInformation<?>> DATA_TYPE = ConfigOptions.key("data-type")
            .type()
            .noDefaultValue()
            .withDescription(
                    Description.builder()
                            .text("The data type that the source connector will produce.")
                            .linebreak()
                            .text("Example: ")
                            .list(
                                    TextElement.text(
                                            "Working with non-parameterized types(TypeInformation<String>):"
                                                    + " 'java.lang.String'"),
                                    TextElement.text(
                                            "Working with parameterized types(TypeInformation<Tuple2<String, Integer>>):"
                                                    + " 'org.apache.flink.api.java.tuple.Tuple2,java.lang.String,java.lang.Integer'")
                            )
                            .build());
    ConfigOption<String> NAME = ConfigOptions.key("name")
            .stringType()
            .defaultValue("NoName")
            .withDescription("The name of the source connector.");

    ConfigOption<Integer> PARALLELISM = ConfigOptions.key("parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("The level of parallelism for the source operator.");
}
