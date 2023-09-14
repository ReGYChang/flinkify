package com.regy.quantalink.common.utils;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.util.DateUtils;

import java.time.LocalDateTime;

/**
 * @author regy
 */
public class TypeUtils extends com.alibaba.fastjson2.util.TypeUtils {

    public static String toString(Object value) {
        return value == null ? null : value.toString();
    }

    public static LocalDateTime toDateTime(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else if (value instanceof String) {
            return DateUtils.parseLocalDateTime((String) value);
        } else if (value instanceof Number) {
            long millis = ((Number) value).longValue();
            if (TimeUtils.isEpochSecond(millis)) {
                millis *= 1000L;
            }
            return TimeUtils.toDateTime(millis);
        } else {
            return null;
        }
    }

    public static Double toDouble(Object value) throws JSONException {
        if (value != null && !(value instanceof Double)) {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else if (value instanceof String) {
                String str = (String) value;
                try {
                    return !str.isEmpty() && !"null".equals(str) ? Double.parseDouble(str) : null;
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                throw new JSONException("can not cast to decimal");
            }
        } else {
            return (Double) value;
        }
    }

    public static double toDoubleValue(Object value) {
        if (value == null) {
            return 0.0;
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            String str = (String) value;
            try {
                return !str.isEmpty() && !"null".equals(str) ? Double.parseDouble(str) : 0.0;
            } catch (NumberFormatException e) {
                return 0.0;
            }
        } else {
            throw new JSONException("can not cast to decimal");
        }
    }
}
