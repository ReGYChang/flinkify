package io.github.regychang.flinkify.common.type;

import io.github.regychang.flinkify.common.utils.CopyUtils;

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@EqualsAndHashCode
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
