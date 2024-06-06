package io.github.regychang.flinkify.flink.core.connector.mongo.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;

import java.util.List;

public interface MongoOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("mongo")
            .configType()
            .asList()
            .noDefaultValue();

    ConfigOption<String> URI = ConfigOptions.key("uri")
            .stringType()
            .noDefaultValue()
            .withDescription("The MongoDB connection URI.");

    ConfigOption<String> DATABASE = ConfigOptions.key("database")
            .stringType()
            .noDefaultValue()
            .withDescription("The MongoDB database name.");

    ConfigOption<String> COLLECTION = ConfigOptions.key("collection")
            .stringType()
            .noDefaultValue()
            .withDescription("The MongoDB collection name.");

    ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size")
            .intType()
            .defaultValue(1000)
            .withDescription("The number of records to buffer before writing them to MongoDB in a single batch.");

    ConfigOption<Long> BATCH_INTERVAL_MS = ConfigOptions.key("batch-interval-ms")
            .longType()
            .defaultValue(1000L)
            .withDescription(
                    "The time interval in milliseconds to wait before writing records to MongoDB if the batch"
                            + " size has not been reached.");

    ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("The maximum number of retries for writing records to MongoDB in case of failures.");
}
