package com.regy.quantalink.quickstart.connector.mongo.serialization;

import com.regy.quantalink.common.type.TypeInformation;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author regy
 */
public class PayloadDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PayloadDeserializationSchema.class);
    private final TypeInformation<T> typeInformation;

    public PayloadDeserializationSchema(TypeInformation<T> typeInformation) {
        this.typeInformation = typeInformation;
    }

    @Override
    public T deserialize(byte[] bytes) {
        try {
            JSONObject jsonObj = JSON.parseObject(bytes);
            JSONArray payload = jsonObj.getJSONArray("payload");

            if (typeInformation.isParameterizedType()) {
                return JSON.parseObject(String.valueOf(payload.getJSONObject(0)), typeInformation.getType());
            }
            return JSON.parseObject(String.valueOf(payload.getJSONObject(0)), typeInformation.getRawType());
        } catch (JSONException e) {
            LOG.error("Failed to parse payload from JSON: {}", bytes, e);
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
