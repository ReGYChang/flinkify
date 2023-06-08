package com.regy.quantalink.flink.core.connector.rabbitmq.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.rabbitmq.func.ComputePropertiesFunc;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author regy
 */
public class RabbitmqSerializationAdapter<T> implements SerializationAdapter<T> {
    private final SerializationSchema<T> serializationSchema;
    private final TypeInformation<T> typeInfo;
    private final ComputePropertiesFunc<T> computePropertiesFunc;

    public RabbitmqSerializationAdapter(
            SerializationSchema<T> serializationSchema,
            TypeInformation<T> typeInfo,
            ComputePropertiesFunc<T> computePropertiesFunc) {

        this.serializationSchema = serializationSchema;
        this.typeInfo = typeInfo;
        this.computePropertiesFunc = computePropertiesFunc;
    }

    public ComputePropertiesFunc<T> getComputePropertiesFunc() {
        return computePropertiesFunc;
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
