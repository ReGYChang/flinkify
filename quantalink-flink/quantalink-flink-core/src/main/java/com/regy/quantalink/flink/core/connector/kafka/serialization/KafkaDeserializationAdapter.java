package com.regy.quantalink.flink.core.connector.kafka.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.DefaultDeserializationSchema;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.Map;

/**
 * @author regy
 */
public class KafkaDeserializationAdapter<T> implements DeserializationAdapter<T> {

    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final TypeInformation<T> typeInfo;

    private KafkaDeserializationAdapter(KafkaRecordDeserializationSchema<T> deserializationSchema, TypeInformation<T> typeInfo) {
        this.deserializationSchema = deserializationSchema;
        this.typeInfo = typeInfo;
    }

    public static <T> KafkaDeserializationAdapter<T> of(KafkaDeserializationSchema<T> kafkaDeserializationSchema, TypeInformation<T> typeInfo) {
        return new KafkaDeserializationAdapter<>(KafkaRecordDeserializationSchema.of(kafkaDeserializationSchema), typeInfo);
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnly(DeserializationSchema<T> valueDeserializationSchema) {
        TypeInformation<T> typeInfo = TypeInformation.get(valueDeserializationSchema.getProducedType());
        return new KafkaDeserializationAdapter<>(KafkaRecordDeserializationSchema.valueOnly(valueDeserializationSchema), typeInfo);
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnlyDefault(TypeInformation<T> typeInformation) {
        DefaultDeserializationSchema<T> deserializationSchema = new DefaultDeserializationSchema<>(typeInformation);
        return new KafkaDeserializationAdapter<>(KafkaRecordDeserializationSchema.valueOnly(deserializationSchema), typeInformation);
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnly(Class<? extends Deserializer<T>> valueDeserializerClass, TypeInformation<T> typeInfo) {
        return valueOnly(valueDeserializerClass, Collections.emptyMap(), typeInfo);
    }

    public static <T, D extends Deserializer<T>> KafkaDeserializationAdapter<T> valueOnly(Class<D> valueDeserializerClass, Map<String, String> config, TypeInformation<T> typeInfo) {
        return new KafkaDeserializationAdapter<>(KafkaRecordDeserializationSchema.valueOnly(valueDeserializerClass, config), typeInfo);
    }

    @SuppressWarnings("unchecked")
    @Override
    public KafkaRecordDeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }

    @Override
    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }
}
