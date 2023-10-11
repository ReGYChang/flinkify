package com.regy.quantalink.flink.core.connector.kafka.source;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.config.OffsetInitializationType;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;

import org.apache.flink.connector.kafka.source.KafkaSource;
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
    private final Long offsetInitializationTimestamp;
    private final OffsetInitializationType offsetInitializationType;

    public KafkaSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.groupId = config.get(KafkaOptions.GROUP_ID);
        this.offsetResetStrategy = config.get(KafkaOptions.OFFSET_RESET_STRATEGY);
        this.offsetInitializationTimestamp = config.get(KafkaOptions.OFFSET_INITIALIZATION_TIMESTAMP);
        this.offsetInitializationType = config.get(KafkaOptions.OFFSET_INITIALIZATION_TYPE);
        this.bootStrapServers = config.getNotNull(KafkaOptions.BOOTSTRAP_SERVERS);
        this.topics = config.getNotNull(KafkaOptions.TOPIC);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        try {
            KafkaDeserializationAdapter<T> deserializer =
                    Optional.ofNullable((KafkaDeserializationAdapter<T>) getDeserializationAdapter())
                            .orElseGet(
                                    () ->
                                            KafkaDeserializationAdapter.valueOnlyDefault(getTypeInfo()));

            return getEnv()
                    .fromSource(
                            createKafkaSource(deserializer),
                            getWatermarkStrategy(),
                            getName())
                    .setParallelism(getParallelism());
        } catch (ClassCastException e) {
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("Kafka source connector '%s' deserialization adapter must be '%s'," +
                                    " could not assign other deserialization adapter",
                            getName(), KafkaDeserializationAdapter.class), e);
        } catch (Exception e) {
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("Could not get source from kafka source connector '%s': ",
                            getName()), e);
        }
    }

    private KafkaSource<T> createKafkaSource(KafkaDeserializationAdapter<T> deserializer) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(
                        offsetInitializationType
                                .withResetStrategy(offsetResetStrategy)
                                .withTimestamp(offsetInitializationTimestamp)
                                .toOffsetsInitializer())
                .setDeserializer(deserializer.getDeserializationSchema())
                .build();
    }
}
