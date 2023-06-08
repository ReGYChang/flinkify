package com.regy.quantalink.flink.core.connector.rabbitmq.func;

import com.rabbitmq.client.AMQP;

import java.io.Serializable;

/**
 * @author regy
 */
@FunctionalInterface
public interface ComputePropertiesFunc<IN> extends Serializable {
    AMQP.BasicProperties.Builder compute(IN input);
}
