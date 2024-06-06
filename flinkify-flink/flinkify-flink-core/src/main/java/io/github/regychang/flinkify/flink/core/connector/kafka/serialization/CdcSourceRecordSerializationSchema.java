package io.github.regychang.flinkify.flink.core.connector.kafka.serialization;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.connector.kafka.config.KafkaOptions;
import io.github.regychang.flinkify.flink.core.connector.kafka.sink.CachingTopicSelector;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;

import javax.annotation.Nullable;
import java.util.HashMap;

public class CdcSourceRecordSerializationSchema implements KafkaRecordSerializationSchema<SourceRecord> {

    private transient JsonConverter jsonConverter;

    private final CachingTopicSelector<SourceRecord> topicSelector;

    public CdcSourceRecordSerializationSchema(CachingTopicSelector<SourceRecord> topicSelector) {
        this.topicSelector = topicSelector;
    }

    public CdcSourceRecordSerializationSchema(Configuration config) {
        String topic = config.getNotNull(KafkaOptions.TOPIC);
        this.topicSelector = new CachingTopicSelector<>(e -> topic);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SourceRecord record, KafkaSinkContext ctx, Long timestamp) {
        if (this.jsonConverter == null) {
            initializeJsonConverter();
        }

        return new ProducerRecord<>(
                this.topicSelector.apply(record),
                this.jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key()),
                this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()));
    }

    private void initializeJsonConverter() {
        this.jsonConverter = new JsonConverter();
        HashMap<String, Object> configs = new HashMap<>(2);
        configs.put("converter.type", ConverterType.VALUE.getName());
        configs.put("schemas.enable", true);
        this.jsonConverter.configure(configs);
    }
}
