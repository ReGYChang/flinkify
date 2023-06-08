package com.regy.quantalink.flink.core.connector.rabbitmq.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;

/**
 * @author regy
 */
public class RabbitmqDeserializationAdapter<T> implements DeserializationAdapter<T> {
    private final DeserializationSchema<T> deserializationSchema;
    private final TypeInformation<T> typeInfo;

    public RabbitmqDeserializationAdapter(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.typeInfo = TypeInformation.get(deserializationSchema.getProducedType());
    }

    @SuppressWarnings("unchecked")
    @Override
    public DeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }

    @Override
    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }
}
