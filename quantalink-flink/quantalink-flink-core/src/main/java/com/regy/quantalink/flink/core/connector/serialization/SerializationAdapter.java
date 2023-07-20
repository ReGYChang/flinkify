package com.regy.quantalink.flink.core.connector.serialization;

import com.regy.quantalink.common.type.TypeInformation;

/**
 * @author regy
 */
public abstract class SerializationAdapter<T, S> {
    private final TypeInformation<T> typeInfo;

    protected SerializationAdapter(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    public abstract S getSerializationSchema();

    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }
}
