package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.type.TypeInformation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public abstract class Connector {

    protected final TypeInformation<?> typeInfo;
    protected final StreamExecutionEnvironment env;
    protected final Configuration config;
    protected final int parallelism;

    public Connector(StreamExecutionEnvironment env, Configuration config, int parallelism, TypeInformation<?> typeInfo) {
        this.env = env;
        this.config = config;
        this.parallelism = parallelism;
        this.typeInfo = typeInfo;
    }

    protected TypeInformation<?> getTypeInfo() {
        return this.typeInfo;
    }

    public abstract void init();
}
