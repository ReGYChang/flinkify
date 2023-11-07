package com.regy.quantalink.flink.core.connector.kafka.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ConfigurationException;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultSerializationSchema;

import io.vavr.control.Try;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * @author regy
 */
public class KafkaSinkConnector<T> extends SinkConnector<T, T> {

    private final String bootStrapServers;
    private final String topic;

    public KafkaSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.bootStrapServers = config.getNotNull(KafkaOptions.BOOTSTRAP_SERVERS, String.format("Kafka sink connector '%s' bootstrap servers must not be null", getName()));
        this.topic = config.get(KafkaOptions.TOPICS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        return Try.of(() -> {

            KafkaSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((KafkaSerializationAdapter<T>) getSerializationAdapter())
                            .orElse(new KafkaSerializationAdapter<>(new DefaultSerializationSchema<>(), getOutputType()));

            KafkaRecordSerializationSchemaBuilder<T> serializerBuilder =
                    KafkaRecordSerializationSchema.builder().setValueSerializationSchema(serializationAdapter.getSerializationSchema());

            KafkaRecordSerializationSchema<T> serializer =
                    Optional.ofNullable(topic)
                            .map(t -> serializerBuilder.setTopic(t).build())
                            .orElseGet(() ->
                                    serializerBuilder.setTopicSelector(Optional.ofNullable(serializationAdapter.getTopicSelector())
                                            .orElseThrow(() -> new ConfigurationException(ErrCode.STREAMING_CONNECTOR_FAILED, ""))).build());

            KafkaSink<T> sink =
                    KafkaSink.<T>builder()
                            .setBootstrapServers(bootStrapServers)
                            .setRecordSerializer(serializer)
                            .build();

            return stream.sinkTo(sink);
        }).getOrElseThrow(e -> {
            if (e instanceof ClassCastException) {
                return new FlinkException(
                        ErrCode.STREAMING_CONNECTOR_FAILED,
                        String.format("Kafka serialization adapter of sink connector '%s' must be '%s', could not assign other adapter",
                                getName(), KafkaSerializationAdapter.class), e);
            } else {
                return new FlinkException(
                        ErrCode.STREAMING_CONNECTOR_FAILED,
                        String.format("Failed to initialize Kafka sink connector '%s'", getName()), e);
            }
        });
    }
}
