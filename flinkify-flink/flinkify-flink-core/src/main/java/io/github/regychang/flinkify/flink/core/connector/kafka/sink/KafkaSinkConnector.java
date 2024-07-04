package io.github.regychang.flinkify.flink.core.connector.kafka.sink;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SinkConnector;
import io.github.regychang.flinkify.flink.core.connector.kafka.config.KafkaOptions;
import io.github.regychang.flinkify.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;
import io.github.regychang.flinkify.flink.core.connector.serialization.DefaultSerializationSchema;

import io.vavr.control.Try;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;
import java.util.Properties;

public class KafkaSinkConnector<T> extends SinkConnector<T, T> {

    private final String bootStrapServers;

    private final Properties properties;

    public KafkaSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.properties = config.get(KafkaOptions.PROPERTIES).toProperties();
        this.bootStrapServers =
                config.getNotNull(
                        KafkaOptions.BOOTSTRAP_SERVERS,
                        String.format("Kafka sink connector '%s' bootstrap servers must not be null", getName()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        return Try.of(() -> {

            KafkaSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((KafkaSerializationAdapter<T>) getSerializationAdapter())
                            .orElse(
                                    new KafkaSerializationAdapter<>(
                                            new DefaultSerializationSchema<>(), getOutputType(), getConfig()));

            KafkaSink<T> sink =
                    KafkaSink.<T>builder()
                            .setBootstrapServers(bootStrapServers)
                            .setRecordSerializer(serializationAdapter.getSerializationSchema())
                            .setKafkaProducerConfig(properties)
                            .build();

            return stream.sinkTo(sink);
        }).getOrElseThrow(
                e -> {
                    if (e instanceof ClassCastException) {
                        return new FlinkException(
                                ErrCode.STREAMING_CONNECTOR_FAILED,
                                String.format(
                                        "Kafka serialization adapter of sink connector '%s' must be '%s'," +
                                                " could not assign other adapter",
                                        getName(),
                                        KafkaSerializationAdapter.class),
                                e);
                    } else {
                        return new FlinkException(
                                ErrCode.STREAMING_CONNECTOR_FAILED,
                                String.format("Failed to initialize Kafka sink connector '%s'", getName()), e);
                    }
                });
    }
}
