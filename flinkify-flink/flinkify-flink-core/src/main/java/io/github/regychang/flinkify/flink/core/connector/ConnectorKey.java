package io.github.regychang.flinkify.flink.core.connector;

import io.github.regychang.flinkify.common.type.TypeInformation;

import lombok.Data;

@Data
public class ConnectorKey<T> {

    private final String id;

    private final TypeInformation<T> typeInformation;
}
