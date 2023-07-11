package com.regy.quantalink.flink.core.connector.mongo.serialization;

import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.utils.TimeUtils;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * @author regy
 */
public class BsonDocumentParser {

    public static BsonDocument parse(Object object) throws FlinkException, IllegalAccessException {
        BsonDocument document = new BsonDocument();

        for (Field field : object.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object fieldValue = field.get(object);

            BsonValue bsonValue = toBsonValue(fieldValue, object);
            document.put(field.getName(), bsonValue);
        }

        return document;
    }

    private static BsonValue toBsonValue(Object object, Object originalObject) throws FlinkException {
        if (object instanceof String) {
            return new BsonString((String) object);
        } else if (object instanceof byte[]) {
            return new BsonString(new String((byte[]) object, StandardCharsets.UTF_8));
        } else if (object instanceof Integer) {
            return new BsonInt32((Integer) object);
        } else if (object instanceof Long) {
            return new BsonInt64((Long) object);
        } else if (object instanceof Double) {
            return new BsonDouble((Double) object);
        } else if (object instanceof Boolean) {
            return new BsonBoolean((Boolean) object);
        } else if (object instanceof LocalDateTime) {
            return new BsonDateTime(TimeUtils.toMillis((LocalDateTime) object));
        } else if (object instanceof BigDecimal) {
            return new BsonDecimal128(new Decimal128((BigDecimal) object));
        } else if (object instanceof List) {
            return createBsonArray((List<?>) object, originalObject);
        } else if (object instanceof Map) {
            return createBsonDocument((Map<?, ?>) object, originalObject);
        } else {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to parse data into BsonDocument, value '%s' is not supported", object));
        }
    }

    private static BsonArray createBsonArray(List<?> list, Object originalObject) throws FlinkException {
        BsonArray bsonArray = new BsonArray();
        for (Object obj : list) {
            if (obj == originalObject) {
                throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to parse data '%s' into BsonDocument, circular reference detected", originalObject));
            }
            bsonArray.add(toBsonValue(obj, originalObject));
        }
        return bsonArray;
    }

    private static BsonDocument createBsonDocument(Map<?, ?> map, Object originalObject) throws FlinkException {
        BsonDocument subDoc = new BsonDocument();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getValue() == originalObject) {
                throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to parse data '%s' into BsonDocument, circular reference detected", originalObject));
            }
            subDoc.put(entry.getKey().toString(), toBsonValue(entry.getValue(), originalObject));
        }
        return subDoc;
    }
}
