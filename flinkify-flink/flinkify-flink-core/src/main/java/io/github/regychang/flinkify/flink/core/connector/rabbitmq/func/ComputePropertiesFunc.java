package io.github.regychang.flinkify.flink.core.connector.rabbitmq.func;

import com.rabbitmq.client.AMQP;

import java.io.Serializable;

@FunctionalInterface
public interface ComputePropertiesFunc<IN> extends Serializable {
    AMQP.BasicProperties.Builder compute(IN input);
}
