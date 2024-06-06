package io.github.regychang.flinkify.flink.core.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.type.TypeInformation;

import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

public interface SourceConnectorOptions {

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
}
