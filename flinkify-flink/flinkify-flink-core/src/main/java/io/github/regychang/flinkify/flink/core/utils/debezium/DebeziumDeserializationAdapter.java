package io.github.regychang.flinkify.flink.core.utils.debezium;

import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;

public class DebeziumDeserializationAdapter<T>
        extends DeserializationAdapter<T, DebeziumDeserializationSchema<T>> {

    private final DebeziumDeserializationSchema<T> deserializationSchema;

    public DebeziumDeserializationAdapter(DebeziumDeserializationSchema<T> deserializationSchema) {
        super(TypeInformation.get(deserializationSchema.getProducedType()));
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public DebeziumDeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }
}
