package com.regy.quantalink.flink.core.connector.rabbitmq.source;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.serialization.RabbitmqDeserializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultDeserializationSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Optional;

/**
 * @author regy
 */
public class RabbitmqSourceConnector<T> extends SourceConnector<T> {

    private final Boolean usesCorrelationId;
    private final String queueName;
    private final RMQConnectionConfig connectionConfig;

    public RabbitmqSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        String host = config.getNotNull(RabbitmqOptions.HOST, String.format("Rabbitmq sink connector '%s' host must not be null", super.name));
        Integer port = config.getNotNull(RabbitmqOptions.PORT, String.format("Rabbitmq sink connector '%s' port must not be null", super.name));
        String virtualHost = config.getNotNull(RabbitmqOptions.VIRTUAL_HOST, String.format("Rabbitmq sink connector '%s' virtual host must not be null", super.name));
        String username = config.getNotNull(RabbitmqOptions.USERNAME, String.format("Rabbitmq sink connector '%s' username must not be null", super.name));
        String password = config.getNotNull(RabbitmqOptions.PASSWORD, String.format("Rabbitmq sink connector '%s' password must not be null", super.name));
        this.usesCorrelationId = config.get(RabbitmqOptions.USES_CORRELATION_ID);
        this.connectionConfig = new RMQConnectionConfig.Builder().setHost(host).setPort(port).setVirtualHost(virtualHost).setUserName(username).setPassword(password).build();
        this.queueName = config.getNotNull(RabbitmqOptions.QUEUE_NAME, String.format("Rabbitmq sink connector '%s' queue-name must not be null", super.name));
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        try {
            RabbitmqDeserializationAdapter<T> deserializationAdapter =
                    Optional.ofNullable((RabbitmqDeserializationAdapter<T>) super.deserializationAdapter)
                            .orElse(new RabbitmqDeserializationAdapter<>(new DefaultDeserializationSchema<>(super.typeInfo)));
            return super.env.addSource(
                    new RMQSource<>(connectionConfig, queueName, usesCorrelationId,
                            deserializationAdapter.getDeserializationSchema()), super.name);
        } catch (ClassCastException e1) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Rabbitmq source connector '%s' deserialization adapter must be '%s', could not assign other deserialization adapter", super.name, RabbitmqDeserializationAdapter.class), e1);
        } catch (Exception e2) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to initialize Rabbitmq sink connector '%s'", super.name), e2);
        }
    }
}
