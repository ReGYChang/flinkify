package com.regy.quantalink.flink.core.connector.serialization;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSerializationSchema<T> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSerializationSchema.class);

    @Override
    public byte[] serialize(T value) {
        try {
            return JSON.toJSONBytes(value);
        } catch (Exception e) {
            LOG.error("Failed to serialize data: {}", value, e);
            return null;
        }
    }
}
