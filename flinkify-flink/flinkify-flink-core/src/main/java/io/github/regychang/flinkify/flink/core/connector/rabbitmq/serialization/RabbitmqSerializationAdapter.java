package io.github.regychang.flinkify.flink.core.connector.rabbitmq.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.func.ComputePropertiesFunc;
import io.github.regychang.flinkify.flink.core.connector.serialization.SerializationAdapter;

import lombok.Getter;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class RabbitmqSerializationAdapter<T> extends SerializationAdapter<T, SerializationSchema<T>> {

    private final SerializationSchema<T> serializationSchema;

    @Getter
    private final ComputePropertiesFunc<T> computePropertiesFunc;

    public RabbitmqSerializationAdapter(
            SerializationSchema<T> serializationSchema,
            ComputePropertiesFunc<T> computePropertiesFunc,
            TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.serializationSchema = serializationSchema;
        this.computePropertiesFunc = computePropertiesFunc;
    }

    @Override
    public SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
