package com.regy.quantalink.flink.core.connector.rabbitmq.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.config.ConnectorOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.func.ComputePropertiesFunc;

import com.rabbitmq.client.AMQP;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author regy
 */
public class RabbitmqSinkPublishOptions<T> implements RMQSinkPublishOptions<T> {
    private final String routingKey;
    private final String exchange;
    private final Boolean mandatory;
    private final Boolean immediate;
    private final Configuration properties;
    private final ComputePropertiesFunc<T> computePropertiesFunc;

    public RabbitmqSinkPublishOptions(Configuration config, ComputePropertiesFunc<T> computePropertiesFunc) {
        String sinkName = config.get(ConnectorOptions.NAME);
        this.routingKey = config.getNotNull(RabbitmqOptions.ROUTING_KEY, String.format("Rabbitmq sink connector '%s' routing-key must not be null", sinkName));
        this.exchange = config.getNotNull(RabbitmqOptions.EXCHANGE, String.format("Rabbitmq sink connector '%s' exchange must not be null", sinkName));
        this.mandatory = config.getNotNull(RabbitmqOptions.MANDATORY, String.format("Rabbitmq sink connector '%s' mandatory must not be null", sinkName));
        this.immediate = config.getNotNull(RabbitmqOptions.IMMEDIATE, String.format("Rabbitmq sink connector '%s' immediate must not be null", sinkName));
        this.properties = config.get(RabbitmqOptions.PROPERTIES);
        this.computePropertiesFunc = computePropertiesFunc;
    }

    @Override
    public String computeRoutingKey(T record) {
        return routingKey;
    }

    @Override
    public String computeExchange(T record) {
        return exchange;
    }

    @Override
    public boolean computeMandatory(T record) {
        return mandatory;
    }

    @Override
    public boolean computeImmediate(T record) {
        return immediate;
    }

    @Override
    public AMQP.BasicProperties computeProperties(T record) {
        AMQP.BasicProperties.Builder propertiesBuilder = computePropertiesFunc.compute(record);
        Set<Map.Entry<String, Object>> entries = properties.toMap().entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            propertiesBuilder.headers(Collections.singletonMap(entry.getKey(), entry.getValue()));
        }
        return propertiesBuilder.build();
    }
}

