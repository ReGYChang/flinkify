package com.regy.quantalink.flink.core.connector.rabbitmq.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.rabbitmq.func.ComputePropertiesFunc;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author regy
 */
public class RabbitmqSerializationAdapter<T> extends SerializationAdapter<T, SerializationSchema<T>> {
    private final SerializationSchema<T> serializationSchema;
    private final ComputePropertiesFunc<T> computePropertiesFunc;

    public RabbitmqSerializationAdapter(SerializationSchema<T> serializationSchema, ComputePropertiesFunc<T> computePropertiesFunc, TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.serializationSchema = serializationSchema;
        this.computePropertiesFunc = computePropertiesFunc;
    }

    public ComputePropertiesFunc<T> getComputePropertiesFunc() {
        return computePropertiesFunc;
    }

    @Override
    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
