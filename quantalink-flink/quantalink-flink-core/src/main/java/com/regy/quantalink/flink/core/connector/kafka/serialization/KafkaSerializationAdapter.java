package com.regy.quantalink.flink.core.connector.kafka.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.TopicSelector;

/**
 * @author regy
 */
@Getter
@Setter
public class KafkaSerializationAdapter<T> extends SerializationAdapter<T, SerializationSchema<T>> {
    private final SerializationSchema<T> serializationSchema;
    private TopicSelector<T> topicSelector;

    public KafkaSerializationAdapter(SerializationSchema<T> serializationSchema, TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
