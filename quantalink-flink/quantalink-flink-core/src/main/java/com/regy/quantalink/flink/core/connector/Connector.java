package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.ConnectorOptions;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 * @author regy
 */
public abstract class Connector<T> implements Serializable {

    protected final TypeInformation<T> typeInfo;
    protected final int parallelism;
    protected final String name;
    protected transient final StreamExecutionEnvironment env;
    protected final Configuration config;

    @SuppressWarnings("unchecked")
    public Connector(StreamExecutionEnvironment env, Configuration config) {
        this.env = env;
        this.config = config;
        this.parallelism = config.get(ConnectorOptions.PARALLELISM);
        this.name = config.get(ConnectorOptions.NAME);
        this.typeInfo = (TypeInformation<T>) config.getNotNull(ConnectorOptions.DATA_TYPE, String.format("Connector '%s' data type must not be null", name));
    }
}
