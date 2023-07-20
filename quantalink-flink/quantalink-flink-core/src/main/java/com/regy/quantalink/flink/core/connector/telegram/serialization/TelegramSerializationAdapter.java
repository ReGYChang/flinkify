package com.regy.quantalink.flink.core.connector.telegram.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;
import com.regy.quantalink.flink.core.connector.telegram.sink.TelegramPayload;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author regy
 */
public class TelegramSerializationAdapter extends SerializationAdapter<TelegramPayload, SerializationSchema<TelegramPayload>> {

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
