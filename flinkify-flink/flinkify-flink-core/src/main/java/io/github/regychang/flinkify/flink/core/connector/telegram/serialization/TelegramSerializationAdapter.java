package io.github.regychang.flinkify.flink.core.connector.telegram.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.SerializationAdapter;
import io.github.regychang.flinkify.flink.core.connector.telegram.sink.TelegramPayload;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class TelegramSerializationAdapter
        extends SerializationAdapter<TelegramPayload, SerializationSchema<TelegramPayload>> {

    private final SerializationSchema<TelegramPayload> serializer;

    public TelegramSerializationAdapter(SerializationSchema<TelegramPayload> serializer) {
        super(TypeInformation.get(TelegramPayload.class));
        this.serializer = serializer;
    }

    @Override
    public SerializationSchema<TelegramPayload> getSerializationSchema() {
        return this.serializer;
    }
}
