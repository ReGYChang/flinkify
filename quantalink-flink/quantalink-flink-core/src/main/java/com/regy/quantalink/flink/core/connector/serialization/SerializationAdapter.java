package com.regy.quantalink.flink.core.connector.serialization;

import com.regy.quantalink.common.type.TypeInformation;

/**
 * @author regy
 */
public interface SerializationAdapter<T> {

    <D> D getSerializationSchema();

    TypeInformation<T> getTypeInfo();
}
