package io.github.regychang.flinkify.flink.core.connector.mongo.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;

public class MongoSerializationAdapter<T> extends SerializationAdapter<T, MongoSerializationSchema<T>> {

    private final MongoSerializationSchema<T> serializationSchema;

    public MongoSerializationAdapter(MongoSerializationSchema<T> serializationSchema, TypeInformation<T> typeInfo) {
        super(typeInfo);
        this.serializationSchema = serializationSchema;
    }

    @Override
    public MongoSerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
