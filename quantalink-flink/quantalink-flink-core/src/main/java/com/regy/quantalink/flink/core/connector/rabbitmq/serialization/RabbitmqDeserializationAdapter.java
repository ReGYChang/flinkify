package com.regy.quantalink.flink.core.connector.rabbitmq.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;

/**
 * @author regy
 */
public class RabbitmqDeserializationAdapter<T> extends DeserializationAdapter<T, DeserializationSchema<T>> {
    private final DeserializationSchema<T> deserializationSchema;

    public RabbitmqDeserializationAdapter(DeserializationSchema<T> deserializationSchema) {
        super(TypeInformation.get(deserializationSchema.getProducedType()));
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public DeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }
}
