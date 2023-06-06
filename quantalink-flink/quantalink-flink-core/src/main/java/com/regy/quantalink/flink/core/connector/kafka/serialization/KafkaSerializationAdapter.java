package com.regy.quantalink.flink.core.connector.kafka.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author regy
 */
public class KafkaSerializationAdapter<T> implements SerializationAdapter<T> {
    private final SerializationSchema<T> serializationSchema;
    private final TypeInformation<T> typeInfo;

    public KafkaSerializationAdapter(SerializationSchema<T> serializationSchema, TypeInformation<T> typeInfo) {
        this.serializationSchema = serializationSchema;
        this.typeInfo = typeInfo;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }

    @Override
    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }
}
