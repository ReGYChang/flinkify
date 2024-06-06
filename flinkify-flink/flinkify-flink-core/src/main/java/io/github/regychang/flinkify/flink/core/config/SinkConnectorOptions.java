package io.github.regychang.flinkify.flink.core.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.utils.JsonFormat;

import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

public interface SinkConnectorOptions {

    ConfigOption<TypeInformation<?>> INPUT_DATA_TYPE = ConfigOptions.key("input-data-type")
            .type()
            .noDefaultValue()
            .withDescription(
                    Description.builder()
                            .text("The input data type that the sink connector will produce.")
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

    ConfigOption<TypeInformation<?>> OUTPUT_DATA_TYPE = ConfigOptions.key("output-data-type")
            .type()
            .noDefaultValue()
            .withDescription(
                    Description.builder()
                            .text("The output data type that the sink connector will produce.")
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

    ConfigOption<JsonFormat> JSON_FORMAT = ConfigOptions.key("json-format")
            .enumType(JsonFormat.class)
            .defaultValue(JsonFormat.JSON)
            .withDescription("");
}
