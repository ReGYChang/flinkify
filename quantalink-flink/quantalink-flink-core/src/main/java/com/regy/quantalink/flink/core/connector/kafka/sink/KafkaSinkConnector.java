package com.regy.quantalink.flink.core.connector.kafka.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;
import com.regy.quantalink.flink.core.connector.serialization.DefaultSerializationSchema;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import com.google.common.base.Preconditions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * @author regy
 */
public class KafkaSinkConnector<T> extends SinkConnector<T> {

    private final String bootStrapServers;
    private final String topic;

    public KafkaSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.bootStrapServers = config.getNotNull(KafkaOptions.BOOTSTRAP_SERVERS, String.format("Kafka sink connector '%s' bootstrap servers must not be null", this.connectorName));
        this.topic = config.getNotNull(KafkaOptions.TOPIC, String.format("Kafka sink connector '%s' topic must not be null", this.connectorName));
    }

    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        SerializationAdapter<T> serializationAdapter = Optional.ofNullable(super.serializationAdapter).orElse(new KafkaSerializationAdapter<>(new DefaultSerializationSchema<>(), super.typeInfo));
        Preconditions.checkArgument(serializationAdapter instanceof KafkaSerializationAdapter, String.format("Kafka sink connector '%s' serialization adapter must be '%s', could not assign other serialization adapter", super.connectorName, KafkaSerializationAdapter.class));
        KafkaSink<T> sink = KafkaSink.<T>builder()
                .setBootstrapServers(bootStrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(((KafkaSerializationAdapter<T>) serializationAdapter).getSerializationSchema()).build()).build();
        return stream.sinkTo(sink).name(super.connectorName).setParallelism(super.parallelism).disableChaining();
    }
}
