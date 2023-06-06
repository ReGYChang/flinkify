package com.regy.quantalink.flink.core.connector.serialization;

import com.regy.quantalink.common.type.TypeInformation;

/**
 * @author regy
 */
public interface DeserializationAdapter<T> {

    <D> D getDeserializationSchema();

    TypeInformation<T> getTypeInfo();
}

