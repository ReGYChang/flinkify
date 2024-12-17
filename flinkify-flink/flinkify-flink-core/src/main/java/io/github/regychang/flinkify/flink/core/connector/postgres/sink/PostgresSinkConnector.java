package io.github.regychang.flinkify.flink.core.connector.postgres.sink;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.AbstractJdbcSinkConnector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresSinkConnector<T> extends AbstractJdbcSinkConnector<T> {

    private static final String DRIVER_NAME = "org.postgresql.Driver";

    private static final String URL = "jdbc:postgresql://%s:%d/%s";

    private static final String INSERT_STATEMENT = "INSERT INTO %s.%s (%s) VALUES (%s)";

    public PostgresSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config, DRIVER_NAME, URL, INSERT_STATEMENT);
    }
}
