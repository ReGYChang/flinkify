package io.github.regychang.flinkify.flink.core.connector.rabbitmq.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;

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
