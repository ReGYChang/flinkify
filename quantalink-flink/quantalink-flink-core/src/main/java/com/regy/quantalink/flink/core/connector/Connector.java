package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.config.ConnectorOptions;

import lombok.Getter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

@Getter
public abstract class Connector implements Serializable {

    private final int parallelism;
    private final String name;
    private transient final StreamExecutionEnvironment env;
    private final Configuration config;

    public Connector(StreamExecutionEnvironment env, Configuration config) {
        this.env = env;
        this.config = config;
        this.parallelism = config.get(ConnectorOptions.PARALLELISM);
        this.name = config.get(ConnectorOptions.NAME);
    }
}
