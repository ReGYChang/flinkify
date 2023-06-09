package com.regy.quantalink.flink.core.connector.kafka.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author regy
 */
public class KafkaSerializationAdapter<T> extends SerializationAdapter<T, SerializationSchema<T>> {
    private final SerializationSchema<T> serializationSchema;

    public KafkaSerializationAdapter(SerializationSchema<T> serializationSchema, TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
