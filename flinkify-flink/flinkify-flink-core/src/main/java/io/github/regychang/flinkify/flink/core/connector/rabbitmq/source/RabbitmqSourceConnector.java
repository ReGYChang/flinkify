package io.github.regychang.flinkify.flink.core.connector.rabbitmq.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.serialization.RabbitmqDeserializationAdapter;
import io.github.regychang.flinkify.flink.core.connector.serialization.DefaultDeserializationSchema;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Optional;

public class RabbitmqSourceConnector<T> extends SourceConnector<T> {

    private final Boolean usesCorrelationId;

    private final String queueName;

    private final RMQConnectionConfig connectionConfig;

    public RabbitmqSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        String host =
                config.getNotNull(
                        RabbitmqOptions.HOST,
                        String.format("Rabbitmq sink connector '%s' host must not be null", getName()));
        Integer port =
                config.getNotNull(
                        RabbitmqOptions.PORT,
                        String.format("Rabbitmq sink connector '%s' port must not be null", getName()));
        String virtualHost =
                config.getNotNull(
                        RabbitmqOptions.VIRTUAL_HOST,
                        String.format("Rabbitmq sink connector '%s' virtual host must not be null", getName()));
        String username =
                config.getNotNull(
                        RabbitmqOptions.USERNAME,
                        String.format("Rabbitmq sink connector '%s' username must not be null", getName()));
        String password =
                config.getNotNull(
                        RabbitmqOptions.PASSWORD,
                        String.format("Rabbitmq sink connector '%s' password must not be null", getName()));
        this.queueName =
                config.getNotNull(
                        RabbitmqOptions.QUEUE_NAME,
                        String.format("Rabbitmq sink connector '%s' queue-name must not be null", getName()));
        this.usesCorrelationId = config.get(RabbitmqOptions.USES_CORRELATION_ID);
        this.connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost(host)
                        .setPort(port)
                        .setVirtualHost(virtualHost)
                        .setUserName(username)
                        .setPassword(password)
                        .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        try {
            RabbitmqDeserializationAdapter<T> deserializationAdapter =
                    Optional.ofNullable((RabbitmqDeserializationAdapter<T>) getDeserializationAdapter())
                            .orElse(
                                    new RabbitmqDeserializationAdapter<>(
                                            new DefaultDeserializationSchema<>(getTypeInfo())));
            return getEnv().addSource(
                    new RMQSource<>(connectionConfig, queueName, usesCorrelationId,
                            deserializationAdapter.getDeserializationSchema()), getName());
        } catch (ClassCastException e) {
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format(
                            "Rabbitmq source connector '%s' deserialization adapter must be '%s'," +
                                    " could not assign other deserialization adapter",
                            getName(), RabbitmqDeserializationAdapter.class), e);
        } catch (Exception e) {
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format(
                            "Failed to initialize Rabbitmq sink connector '%s'", getName()), e);
        }
    }
}
