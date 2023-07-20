package com.regy.quantalink.flink.core.connector.kafka.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultSerializationSchema;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
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
        this.topic = config.getNotNull(KafkaOptions.TOPIC, String.format("Kafka sink connector '%s' topic must not be null", getName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        try {
            KafkaSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((KafkaSerializationAdapter<T>) getSerializationAdapter())
                            .orElse(new KafkaSerializationAdapter<>(new DefaultSerializationSchema<>(), getOutputType()));
            KafkaSink<T> sink = KafkaSink.<T>builder()
                    .setBootstrapServers(bootStrapServers)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic(topic)
                                    .setValueSerializationSchema(serializationAdapter.getSerializationSchema()).build()).build();
            return stream.sinkTo(sink).name(getName()).setParallelism(getParallelism()).disableChaining();
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Kafka sink connector '%s' com.nexdata.flink.traceability.serialization adapter must be '%s', could not assign other com.nexdata.flink.traceability.serialization adapter", getName(), KafkaSerializationAdapter.class), e);
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to initialize Kafka sink connector '%s'", getName()), e);
        }
    }
}
