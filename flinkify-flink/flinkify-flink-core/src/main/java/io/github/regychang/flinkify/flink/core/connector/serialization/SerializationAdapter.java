package io.github.regychang.flinkify.flink.core.connector.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;

import lombok.Getter;

@Getter
public abstract class SerializationAdapter<T, S> {

    private final TypeInformation<T> typeInfo;

    protected SerializationAdapter(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    public abstract S getSerializationSchema();
}
