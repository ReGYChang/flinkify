package com.regy.quantalink.flink.core.connector.rabbitmq.config;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.ConfigOptions;
import com.regy.quantalink.common.config.Configuration;

import java.util.List;

/**
 * @author regy
 */
public interface RabbitmqOptions {

    ConfigOption<List<Configuration>> CONNECTORS = ConfigOptions.key("rabbitmq")
            .configType()
            .asList()
            .noDefaultValue()
            .withDescription("");

    ConfigOption<String> HOST = ConfigOptions.key("host")
            .stringType()
            .noDefaultValue()
            .withDescription("The hostname or IP address of the RabbitMQ server.");

    ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue()
            .withDescription("The port number on which the RabbitMQ server is listening.");

    ConfigOption<String> VIRTUAL_HOST = ConfigOptions.key("virtual-host")
            .stringType()
            .noDefaultValue()
            .withDescription("The virtual host used by RabbitMQ for logical separation of resources.");

    ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("The username for authentication with the RabbitMQ server.");

    ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("The password for authentication with the RabbitMQ server.");

    ConfigOption<String> EXCHANGE = ConfigOptions.key("exchange")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the exchange for routing messages in RabbitMQ.");

    ConfigOption<String> ROUTING_KEY = ConfigOptions.key("routing-key")
            .stringType()
            .noDefaultValue()
            .withDescription("The routing key used to route messages to the appropriate queue.");

    ConfigOption<String> QUEUE_NAME = ConfigOptions.key("queue-name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the queue to which messages will be sent.");

    ConfigOption<Boolean> USES_CORRELATION_ID = ConfigOptions.key("uses-correlation-id")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to use correlation ID for message tracking.");

    ConfigOption<Boolean> MANDATORY = ConfigOptions.key("mandatory")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether messages must be routed to a queue, or returned to the sender if unroutable.");

    ConfigOption<Boolean> IMMEDIATE = ConfigOptions.key("immediate")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether messages must be processed immediately, or returned to the sender if the queue is not ready to accept them.");

    ConfigOption<Configuration> PROPERTIES = ConfigOptions.key("properties")
            .configType()
            .defaultValue(new Configuration())
            .withDescription("A map of additional properties to configure RabbitMQ.");
}
