package com.regy.quantalink.flink.core.connector.mongo.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;

/**
 * @author regy
 */
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
