package io.github.regychang.flinkify.flink.core.connector.kafka.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.DefaultDeserializationSchema;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.Map;

public class KafkaDeserializationAdapter<T>
        extends DeserializationAdapter<T, KafkaRecordDeserializationSchema<T>> {

    private final KafkaRecordDeserializationSchema<T> deserializationSchema;

    private KafkaDeserializationAdapter(KafkaRecordDeserializationSchema<T> deserializationSchema) {
        super(TypeInformation.get(deserializationSchema.getProducedType()));
        this.deserializationSchema = deserializationSchema;
    }

    public static <T> KafkaDeserializationAdapter<T> of(
            KafkaDeserializationSchema<T> kafkaDeserializationSchema) {
        return new KafkaDeserializationAdapter<>(
                KafkaRecordDeserializationSchema.of(kafkaDeserializationSchema));
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnly(
            DeserializationSchema<T> valueDeserializationSchema) {
        return new KafkaDeserializationAdapter<>(
                KafkaRecordDeserializationSchema.valueOnly(valueDeserializationSchema));
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnlyDefault(
            TypeInformation<T> typeInformation) {
        DefaultDeserializationSchema<T> deserializationSchema =
                new DefaultDeserializationSchema<>(typeInformation);
        return new KafkaDeserializationAdapter<>(
                KafkaRecordDeserializationSchema.valueOnly(deserializationSchema));
    }

    public static <T> KafkaDeserializationAdapter<T> valueOnly(
            Class<? extends Deserializer<T>> valueDeserializerClass) {
        return valueOnly(valueDeserializerClass, Collections.emptyMap());
    }

    public static <T, D extends Deserializer<T>> KafkaDeserializationAdapter<T> valueOnly(
            Class<D> valueDeserializerClass, Map<String, String> config) {
        return new KafkaDeserializationAdapter<>(
                KafkaRecordDeserializationSchema.valueOnly(valueDeserializerClass, config));
    }

    @Override
    public KafkaRecordDeserializationSchema<T> getDeserializationSchema() {
        return deserializationSchema;
    }
}
