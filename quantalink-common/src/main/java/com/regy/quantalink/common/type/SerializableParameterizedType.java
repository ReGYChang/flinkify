package com.regy.quantalink.common.type;

import com.regy.quantalink.common.utils.CopyUtils;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author regy
 */
public class SerializableParameterizedType implements ParameterizedType, Serializable {
    private final Type[] actualTypeArguments;
    private final Type rawType;

    public SerializableParameterizedType(Class<?> rawType, Type... actualTypeArguments) {
        this.rawType = rawType;
        this.actualTypeArguments = actualTypeArguments;
    }

    @Override
    public Type[] getActualTypeArguments() {
        return CopyUtils.deepCopy(actualTypeArguments);
    }

    @Override
    public Type getRawType() {
        return rawType;
    }

    @Override
    public Type getOwnerType() {
        return null;
    }
}
