//package com.regy.quantalink.flink.core.config;
//
//import com.regy.quantalink.common.config.ConfigOption;
//import com.regy.quantalink.common.config.ConfigOptions;
//import com.regy.quantalink.common.type.TypeInformation;
//import com.regy.quantalink.flink.core.connector.ConnectorType;
//
//import org.apache.flink.configuration.description.Description;
//import org.apache.flink.configuration.description.TextElement;
//
//
///**
// * @author regy
// */
//public interface SinkOptions {
//
//    ConfigOption<ConnectorType> CONNECTOR_TYPE = ConfigOptions.key("flink.sinks.connector-type")
//            .enumType(ConnectorType.class)
//            .noDefaultValue()
//            .withDescription(
//                    Description.builder()
//                            .text("The type of source connector to be used for data ingestion. Supported sink connector type: ")
//                            .list(
//                                    TextElement.text("kafka"),
//                                    TextElement.text("rabbitmq"),
//                                    TextElement.text("nebula_graph"),
//                                    TextElement.text("mongo_db"))
//                            .build());
//
//    ConfigOption<TypeInformation<?>> DATA_TYPE = ConfigOptions.key("flink.sinks.data-type")
//            .type()
//            .noDefaultValue()
//            .withDescription(
//                    Description.builder()
//                            .text("The data type that the sink connector will accept.")
//                            .linebreak()
//                            .text("Example: ")
//                            .list(
//                                    TextElement.text(
//                                            "Working with non-parameterized types(TypeInformation<String>):"
//                                                    + " 'java.lang.String'"),
//                                    TextElement.text(
//                                            "Working with parameterized types(TypeInformation<Tuple2<String, Integer>>):"
//                                                    + " 'org.apache.flink.api.java.tuple.Tuple2,java.lang.String,java.lang.Integer'")
//                            )
//                            .build());
//
//    ConfigOption<String> NAME = ConfigOptions.key("flink.sinks.name")
//            .stringType()
//            .defaultValue("Unnamed")
//            .withDescription("The name of the sink connector.");
//
//    ConfigOption<Integer> PARALLELISM = ConfigOptions.key("flink.sinks.parallelism")
//            .intType()
//            .defaultValue(1)
//            .withDescription("The level of parallelism for the sink operator.");
//}
