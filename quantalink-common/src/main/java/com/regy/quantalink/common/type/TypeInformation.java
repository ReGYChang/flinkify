package com.regy.quantalink.common.type;

import com.regy.quantalink.common.utils.CopyUtils;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author regy
 */
@ToString
@EqualsAndHashCode
@Getter
public class TypeInformation<T> implements Serializable {
    private final Class<T> rawType;
    private final Type[] actualTypeArguments;
    private Type type;

    private TypeInformation(Class<T> rawType, Type... actualTypeArguments) {
        Preconditions.checkNotNull(rawType, "TypeInformation rawType must not be null");
        this.rawType = rawType;
        this.actualTypeArguments = actualTypeArguments;
        if (actualTypeArguments.length > 0) {
            this.type = new SerializableParameterizedType(rawType, actualTypeArguments);
        }
    }

    public Type[] getActualTypeArguments() {
        return CopyUtils.deepCopy(actualTypeArguments);
    }

    public boolean isParameterizedType() {
        return type != null;
    }

    public org.apache.flink.api.common.typeinfo.TypeInformation<T> toFlinkType() {
        return TypeInformation.convertToFlinkType(this);
    }

    public static <T> TypeInformation<T> get(Class<T> rawType, Type... actualTypeArguments) {
        return new TypeInformation<>(rawType, actualTypeArguments);
    }

    public static <T> TypeInformation<T> get(Class<T> clazz) {
        Type superClass = clazz.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superClass;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            return new TypeInformation<>(clazz, actualTypeArguments);
        } else {
            return new TypeInformation<>(clazz);
        }
    }

    public static <T> TypeInformation<T> get(org.apache.flink.api.common.typeinfo.TypeInformation<T> typeInformation) {
        Type superClass = typeInformation.getClass().getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superClass;
            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            Class<T> rawType = typeInformation.getTypeClass();
            return new TypeInformation<>(rawType, actualTypeArguments);
        } else {
            Class<T> rawType = typeInformation.getTypeClass();
            return new TypeInformation<>(rawType);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> org.apache.flink.api.common.typeinfo.TypeInformation<T> convertToFlinkType(TypeInformation<T> typeInformation) {
        Class<T> rawType = typeInformation.getRawType();
        Type[] actualTypeArguments = typeInformation.getActualTypeArguments();

        if (typeInformation.isParameterizedType()) {
            if (rawType.equals(Tuple2.class) && actualTypeArguments.length == 2) {
                org.apache.flink.api.common.typeinfo.TypeInformation<?> typeInfo1 =
                        convertToFlinkType(TypeInformation.get((Class<?>) actualTypeArguments[0]));
                org.apache.flink.api.common.typeinfo.TypeInformation<?> typeInfo2 =
                        convertToFlinkType(TypeInformation.get((Class<?>) actualTypeArguments[1]));

                return (org.apache.flink.api.common.typeinfo.TypeInformation<T>) Types.TUPLE(typeInfo1, typeInfo2);
            }
            // Add more cases for other parameterized types if needed
        }

        return org.apache.flink.api.common.typeinfo.TypeInformation.of(rawType);
    }
}
