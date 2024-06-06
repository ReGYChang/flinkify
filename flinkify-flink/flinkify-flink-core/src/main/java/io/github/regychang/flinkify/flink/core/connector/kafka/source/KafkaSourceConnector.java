package io.github.regychang.flinkify.flink.core.connector.kafka.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.kafka.config.KafkaOptions;
import io.github.regychang.flinkify.flink.core.connector.kafka.config.OffsetInitializationType;
import io.github.regychang.flinkify.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;

import io.vavr.control.Try;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaSourceConnector<T> extends SourceConnector<T> {

    private final String bootStrapServers;

    private final String topics;

    private final String topicPattern;

    private final String groupId;

    private final OffsetResetStrategy offsetResetStrategy;

    private final Long offsetInitializationTimestamp;

    private final OffsetInitializationType offsetInitializationType;

    private final Properties properties;

    public KafkaSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.groupId = config.get(KafkaOptions.GROUP_ID);
        this.offsetResetStrategy = config.get(KafkaOptions.OFFSET_RESET_STRATEGY);
        this.offsetInitializationTimestamp = config.get(KafkaOptions.OFFSET_INITIALIZATION_TIMESTAMP);
        this.offsetInitializationType = config.get(KafkaOptions.OFFSET_INITIALIZATION_TYPE);
        this.bootStrapServers = config.getNotNull(KafkaOptions.BOOTSTRAP_SERVERS);
        this.topics = config.get(KafkaOptions.TOPICS);
        this.topicPattern = config.get(KafkaOptions.TOPIC_PATTERN);
        this.properties = config.get(KafkaOptions.PROPERTIES).toProperties();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        return Try.of(() -> {
            KafkaDeserializationAdapter<T> deserializer =
                    Optional.ofNullable((KafkaDeserializationAdapter<T>) getDeserializationAdapter())
                            .orElseGet(() -> KafkaDeserializationAdapter.valueOnlyDefault(getTypeInfo()));

            return getEnv()
                    .fromSource(
                            createKafkaSource(deserializer),
                            getWatermarkStrategy(),
                            getName())
                    .setParallelism(getParallelism());

        }).getOrElseThrow(e -> {
            if (e instanceof ClassCastException) {
                return new FlinkException(
                        ErrCode.STREAMING_CONNECTOR_FAILED,
                        String.format("Kafka source connector '%s' deserialization adapter must be '%s'," +
                                " could not assign other deserialization adapter", getName(), KafkaDeserializationAdapter.class), e);
            } else {
                return new FlinkException(
                        ErrCode.STREAMING_CONNECTOR_FAILED,
                        String.format("Could not get source from kafka source connector '%s': ", getName()), e);
            }
        });
    }

    private KafkaSource<T> createKafkaSource(KafkaDeserializationAdapter<T> deserializer) {
        KafkaSourceBuilder<T> builder =
                KafkaSource.<T>builder()
                        .setBootstrapServers(bootStrapServers)
                        .setGroupId(groupId)
                        .setStartingOffsets(
                                offsetInitializationType
                                        .withResetStrategy(offsetResetStrategy)
                                        .withTimestamp(offsetInitializationTimestamp)
                                        .toOffsetsInitializer())
                        .setDeserializer(deserializer.getDeserializationSchema())
                        .setProperties(properties);

        return topics == null ?
                builder.setTopicPattern(Pattern.compile(topicPattern)).build() :
                builder.setTopics(topics).build();
    }
}
