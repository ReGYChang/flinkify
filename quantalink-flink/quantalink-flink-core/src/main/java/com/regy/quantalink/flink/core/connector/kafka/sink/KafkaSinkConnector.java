package com.regy.quantalink.flink.core.connector.kafka.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.config.ConnectorOptions;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;

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
        super(env, config, config.get(ConnectorOptions.PARALLELISM), config.get(ConnectorOptions.NAME), config.getNotNull(ConnectorOptions.DATA_TYPE, "Kafka sink connector data type must not be null"));
        this.bootStrapServers = Preconditions.checkNotNull(config.get(KafkaOptions.BOOTSTRAP_SERVERS), "Kafka sink connector '%s' bootstrap servers must not be null", this.sinkName);
        this.topic = Preconditions.checkNotNull(config.get(KafkaOptions.TOPIC), "Kafka sink connector '%s' topic must not be null", this.sinkName);
    }

    @Override
    public void init() {
    }

    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        Optional.ofNullable(serializationAdapter).orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Kafka sink connector '%s' serializer must not be null", this.sinkName)));
        KafkaSink<T> sink = KafkaSink.<T>builder()
                .setBootstrapServers(bootStrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(((KafkaSerializationAdapter<T>) serializationAdapter).getSerializationSchema()).build()).build();
        return stream.sinkTo(sink).name(this.sinkName).setParallelism(this.parallelism).disableChaining();
    }
}
