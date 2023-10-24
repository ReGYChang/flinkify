package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.type.TypeInformation;

import lombok.Data;

@Data
public class ConnectorKey<T> {
    private final String id;
    private final TypeInformation<T> typeInformation;
}
