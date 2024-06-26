package io.github.regychang.flinkify.flink.core.connector;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.config.ConnectorOptions;

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
