package com.regy.quantalink.flink.core.connector.rabbitmq.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.serialization.RabbitmqSerializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultSerializationSchema;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Optional;

/**
 * @author regy
 */
public class RabbitmqSinkConnector<T> extends SinkConnector<T, T> {

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;

    public RabbitmqSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        String host = config.getNotNull(RabbitmqOptions.HOST, String.format("Rabbitmq sink connector '%s' host must not be null", getName()));
        Integer port = config.getNotNull(RabbitmqOptions.PORT, String.format("Rabbitmq sink connector '%s' port must not be null", getName()));
        String virtualHost = config.getNotNull(RabbitmqOptions.VIRTUAL_HOST, String.format("Rabbitmq sink connector '%s' virtual host must not be null", getName()));
        String username = config.getNotNull(RabbitmqOptions.USERNAME, String.format("Rabbitmq sink connector '%s' username must not be null", getName()));
        String password = config.getNotNull(RabbitmqOptions.PASSWORD, String.format("Rabbitmq sink connector '%s' password must not be null", getName()));
        this.queueName = config.get(RabbitmqOptions.QUEUE_NAME);
        this.connectionConfig = new RMQConnectionConfig.Builder().setHost(host).setPort(port).setVirtualHost(virtualHost).setUserName(username).setPassword(password).build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        try {
            RabbitmqSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((RabbitmqSerializationAdapter<T>) getSerializationAdapter()).orElse(
                            new RabbitmqSerializationAdapter<>(
                                    new DefaultSerializationSchema<>(), null, getOutputType()));
            RabbitmqSinkPublishOptions<T> sinkPublishOpts = new RabbitmqSinkPublishOptions<>(getConfig(), serializationAdapter.getComputePropertiesFunc());
            RMQSink<T> sink =
                    Optional.ofNullable(queueName).isPresent() ?
                            new RMQSink<>(connectionConfig, queueName, serializationAdapter.getSerializationSchema()) :
                            new RMQSink<>(connectionConfig, serializationAdapter.getSerializationSchema(), sinkPublishOpts);
            return stream.addSink(sink).name(getName()).setParallelism(getParallelism()).disableChaining();
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Rabbitmq sink connector '%s' com.nexdata.flink.traceability.serialization adapter must be '%s', could not assign other com.nexdata.flink.traceability.serialization adapter", getName(), RabbitmqSerializationAdapter.class), e);
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to initialize RabbitMQ sink connector '%s'", getName()), e);
        }
    }
}
