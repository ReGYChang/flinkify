package com.regy.quantalink.flink.core.connector.kafka.source;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.config.ConnectorOptions;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Optional;

/**
 * @author regy
 */
public class KafkaSourceConnector<T> extends SourceConnector<T> {

    private final String bootStrapServers;
    private final String topics;
    private final String groupId;
    private final OffsetResetStrategy offsetResetStrategy;

    public KafkaSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config, config.get(ConnectorOptions.PARALLELISM), config.get(ConnectorOptions.NAME), config.getNotNull(ConnectorOptions.DATA_TYPE, "Kafka source connector data type must not be null"));
        this.groupId = config.get(KafkaOptions.GROUP_ID);
        this.offsetResetStrategy = config.get(KafkaOptions.OFFSET_RESET_STRATEGY);
        this.bootStrapServers = config.getNotNull(KafkaOptions.BOOTSTRAP_SERVERS, "Kafka source connector bootstrap servers must not be null, please check your configuration");
        this.topics = config.getNotNull(KafkaOptions.TOPIC, "Kafka source topics must not be null, please check your configuration");
    }

    @Override
    public void init() {

    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        DeserializationAdapter<T> deserializer = Optional.ofNullable(deserializationAdapter).orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, "Kafka source connector deserializer must not be null"));
        WatermarkStrategy<T> watermark = Optional.ofNullable(watermarkStrategy).orElse(WatermarkStrategy.noWatermarks());
        try {
            return env.fromSource(
                            KafkaSource.<T>builder()
                                    .setBootstrapServers(bootStrapServers)
                                    .setTopics(topics)
                                    .setGroupId(groupId)
                                    .setStartingOffsets(OffsetsInitializer.committedOffsets(offsetResetStrategy))
                                    .setDeserializer(deserializer.getDeserializationSchema())
                                    .build(), watermark, sourceName)
                    .setParallelism(parallelism);
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Could not get source from kafka connector '%s': ", super.sourceName), e);
        }
    }
}
