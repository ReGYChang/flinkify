package com.regy.quantalink.flink.core.connector.kafka.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.List;

/**
 * @author regy
 */
public interface KafkaOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("kafka")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("The connector list of Kafka.");

    ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions.key("bootstrap-servers")
            .stringType()
            .noDefaultValue()
            .withDescription("The comma-separated list of Kafka broker addresses.");

    ConfigOption<String> GROUP_ID = ConfigOptions.key("group-id")
            .stringType()
            .defaultValue("flink-default-consumer")
            .withDescription("The consumer group ID used for the Kafka consumer.");

    ConfigOption<String> TOPICS = ConfigOptions.key("topics")
            .stringType()
            .noDefaultValue()
            .withDescription("The Kafka topic to consume from.");

    ConfigOption<String> TOPIC = ConfigOptions.key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("The Kafka topic to sink to.");

    ConfigOption<String> TOPIC_PATTERN = ConfigOptions.key("topic-pattern")
            .stringType()
            .noDefaultValue()
            .withDescription("The Kafka topics whose name matches the provided regular expression to consume from.");

    ConfigOption<Configuration> PROPERTIES = ConfigOptions.key("properties")
            .configType()
            .defaultValue(new Configuration())
            .withDescription("");

    ConfigOption<OffsetResetStrategy> OFFSET_RESET_STRATEGY = ConfigOptions.key("offset-reset-strategy")
            .enumType(OffsetResetStrategy.class)
            .defaultValue(OffsetResetStrategy.EARLIEST)
            .withDescription(
                    "The strategy to use if the Kafka consumer does not find a valid offset for the topic."
                            + "Valid options are 'EARLIEST' and 'LATEST'.");

    ConfigOption<OffsetInitializationType> OFFSET_INITIALIZATION_TYPE = ConfigOptions.key("offset-initialization-type")
            .enumType(OffsetInitializationType.class)
            .defaultValue(OffsetInitializationType.EARLIEST)
            .withDescription(
                    "The type of offset initialization " +
                            "(COMMITTED_WITH_RESET_STRATEGY, TIMESTAMP, EARLIEST, LATEST).");

    ConfigOption<Long> OFFSET_INITIALIZATION_TIMESTAMP = ConfigOptions.key("offset-initialization-timestamp")
            .longType()
            .defaultValue(-1L)
            .withDescription("The timestamp for offset initialization when using the TIMESTAMP strategy." +
                    "Default to -1 which indicates using current timestamp.");
}
