package com.regy.quantalink.flink.core.connector.rabbitmq.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.serialization.RabbitmqSerializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultSerializationSchema;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Optional;

/**
 * @author regy
 */
public class RabbitmqSinkConnector<T> extends SinkConnector<T> {

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;

    public RabbitmqSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        String host = config.getNotNull(RabbitmqOptions.HOST, String.format("Rabbitmq sink connector '%s' host must not be null", super.connectorName));
        Integer port = config.getNotNull(RabbitmqOptions.PORT, String.format("Rabbitmq sink connector '%s' port must not be null", super.connectorName));
        String virtualHost = config.getNotNull(RabbitmqOptions.VIRTUAL_HOST, String.format("Rabbitmq sink connector '%s' virtual host must not be null", super.connectorName));
        String username = config.getNotNull(RabbitmqOptions.USERNAME, String.format("Rabbitmq sink connector '%s' username must not be null", super.connectorName));
        String password = config.getNotNull(RabbitmqOptions.PASSWORD, String.format("Rabbitmq sink connector '%s' password must not be null", super.connectorName));
        this.queueName = config.getNotNull(RabbitmqOptions.QUEUE_NAME, String.format("Rabbitmq sink connector '%s' queue-name must not be null", super.connectorName));
        this.connectionConfig = new RMQConnectionConfig.Builder().setHost(host).setPort(port).setVirtualHost(virtualHost).setUserName(username).setPassword(password).build();
    }

    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        SerializationAdapter<T> serializationAdapter = Optional.ofNullable(super.serializationAdapter).orElse(new RabbitmqSerializationAdapter<>(new DefaultSerializationSchema<>(), super.typeInfo, null));
        Preconditions.checkArgument(serializationAdapter instanceof RabbitmqSerializationAdapter, String.format("Rabbitmq sink connector '%s' serialization adapter must be '%s', could not assign other serialization adapter", super.connectorName, RabbitmqSerializationAdapter.class));
        RabbitmqSinkPublishOptions<T> sinkPublishOpt = new RabbitmqSinkPublishOptions<>(super.config, ((RabbitmqSerializationAdapter<T>) serializationAdapter).getComputePropertiesFunc());
        RMQSink<T> sink =
                Optional.ofNullable(queueName).isPresent() ?
                        new RMQSink<>(connectionConfig, queueName, serializationAdapter.getSerializationSchema()) :
                        new RMQSink<>(connectionConfig, serializationAdapter.getSerializationSchema(), sinkPublishOpt);
        return stream.addSink(sink).name(super.connectorName).setParallelism(super.parallelism).disableChaining();
    }
}
