package io.github.regychang.flinkify.flink.core.connector.sqlserver.sink;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.AbstractJdbcSinkConnector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SqlServerSinkConnector<T> extends AbstractJdbcSinkConnector<T> {

    private static final String DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final String URL = "jdbc:sqlserver://%s:%d;databaseName=%s";

    private static final String INSERT_STATEMENT = "INSERT INTO %s.%s (%s) VALUES (%s)";

    public SqlServerSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config, DRIVER_NAME, URL, INSERT_STATEMENT);
    }
}
