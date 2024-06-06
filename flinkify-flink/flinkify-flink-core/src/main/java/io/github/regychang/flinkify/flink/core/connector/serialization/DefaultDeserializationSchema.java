package io.github.regychang.flinkify.flink.core.connector.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeserializationSchema.class);

    private final TypeInformation<T> typeInformation;

    public DefaultDeserializationSchema(TypeInformation<T> typeInformation) {
        this.typeInformation = typeInformation;
    }

    @Override
    public T deserialize(byte[] bytes) {
        try {
            if (typeInformation.isParameterizedType()) {
                return JSON.parseObject(bytes, typeInformation.getType());
            }
            return JSON.parseObject(bytes, typeInformation.getRawType());
        } catch (JSONException e) {
            LOG.error("Failed to deserialize data with type: {}", typeInformation, e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public org.apache.flink.api.common.typeinfo.TypeInformation<T> getProducedType() {
        return TypeInformation.convertToFlinkType(typeInformation);
    }
}
