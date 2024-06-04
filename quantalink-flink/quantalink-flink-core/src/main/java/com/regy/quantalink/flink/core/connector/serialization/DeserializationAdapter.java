package com.regy.quantalink.flink.core.connector.serialization;

import com.regy.quantalink.common.type.TypeInformation;

import lombok.Getter;

@Getter
public abstract class DeserializationAdapter<T, D> {

    private final TypeInformation<T> typeInfo;

    protected DeserializationAdapter(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    public abstract D getDeserializationSchema();
}
