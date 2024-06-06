package io.github.regychang.flinkify.flink.core.connector.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;

import lombok.Getter;

@Getter
public abstract class DeserializationAdapter<T, D> {

    private final TypeInformation<T> typeInfo;

    protected DeserializationAdapter(TypeInformation<T> typeInfo) {
        this.typeInfo = typeInfo;
    }

    public abstract D getDeserializationSchema();
}
