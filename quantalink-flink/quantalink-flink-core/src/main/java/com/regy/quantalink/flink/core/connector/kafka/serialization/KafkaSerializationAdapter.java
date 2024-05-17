package com.regy.quantalink.flink.core.connector.kafka.serialization;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.type.TypeInformation;

import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.TopicSelector;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

@Getter
@Setter
public class KafkaSerializationAdapter<T> extends SerializationAdapter<T, KafkaRecordSerializationSchema<T>> {

    private final KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema;

    public KafkaSerializationAdapter(
            KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema,
            TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.kafkaRecordSerializationSchema = kafkaRecordSerializationSchema;
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            TopicSelector<T> topicSelector) {
        this(valueSerializationSchema, typeInfo, topicSelector, null, null);
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            TopicSelector<T> topicSelector,
            SerializationSchema<T> keySerializationSchema) {
        this(valueSerializationSchema, typeInfo, topicSelector, null, keySerializationSchema);
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            Configuration config) {
        this(valueSerializationSchema, typeInfo, config, null);
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            Configuration config,
            SerializationSchema<T> keySerializationSchema) {
        this(valueSerializationSchema, typeInfo, config.getNotNull(KafkaOptions.TOPIC), null, keySerializationSchema);
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            String topic,
            FlinkKafkaPartitioner<T> partitioner,
            SerializationSchema<T> keySerializationSchema) {
        super(typeInfo);
        KafkaRecordSerializationSchemaBuilder<T> builder = KafkaRecordSerializationSchema.builder();
        builder.setValueSerializationSchema(valueSerializationSchema);
        builder.setTopic(topic);
        if (keySerializationSchema != null) {
            builder.setKeySerializationSchema(keySerializationSchema);
        }
        if (partitioner != null) {
            builder.setPartitioner(partitioner);
        }
        this.kafkaRecordSerializationSchema = builder.build();
    }

    public KafkaSerializationAdapter(
            SerializationSchema<T> valueSerializationSchema,
            TypeInformation<T> typeInfo,
            TopicSelector<T> topicSelector,
            FlinkKafkaPartitioner<T> partitioner,
            SerializationSchema<T> keySerializationSchema) {
        super(typeInfo);
        KafkaRecordSerializationSchemaBuilder<T> builder = KafkaRecordSerializationSchema.builder();
        builder.setValueSerializationSchema(valueSerializationSchema);
        builder.setTopicSelector(topicSelector);
        if (keySerializationSchema != null) {
            builder.setKeySerializationSchema(keySerializationSchema);
        }
        if (partitioner != null) {
            builder.setPartitioner(partitioner);
        }
        this.kafkaRecordSerializationSchema = builder.build();
    }

    @Override
    public KafkaRecordSerializationSchema<T> getSerializationSchema() {
        return kafkaRecordSerializationSchema;
    }
}