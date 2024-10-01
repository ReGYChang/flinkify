package io.github.regychang.flinkify.flink.core.connector.mysql.cdc;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.mysql.config.MySqlOptions;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import io.github.regychang.flinkify.flink.core.utils.debezium.DeserializationUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlCdcConnector<T> extends SourceConnector<T> {

    private final String hostname;

    private final Integer port;

    private final String databaseList;

    private final String tableList;

    private final String username;

    private final String password;

    private final String serverTimeZone;

    private final boolean includeSchemaChanges;

    public MySqlCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.hostname =
                config.getNotNull(
                        MySqlOptions.HOSTNAME,
                        "MySQL source CDC connector hostname must not be null");
        this.port =
                config.getNotNull(
                        MySqlOptions.PORT, "MySQL source CDC connector port must not be null");
        this.databaseList = config.get(MySqlOptions.DATABASE_LIST);
        this.tableList =
                config.getNotNull(
                        MySqlOptions.TABLE_LIST,
                        "MySQL source CDC connector table list must not be null");
        this.username =
                config.getNotNull(
                        MySqlOptions.USERNAME,
                        "MySQL source CDC connector username must not be null");
        this.password =
                config.getNotNull(
                        MySqlOptions.PASSWORD,
                        "MySQL source CDC connector password must not be null");
        this.serverTimeZone = config.get(MySqlOptions.SERVER_TIME_ZONE);
        this.includeSchemaChanges = config.get(MySqlOptions.INCLUDE_SCHEMA_CHANGES);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        DebeziumDeserializationSchema<T> deserializationSchema =
                DeserializationUtils.extractDeserializationSchema(getDeserializationAdapter());

        return getEnv().fromSource(
                        MySqlSource.<T>builder()
                                .hostname(hostname)
                                .port(port)
                                .databaseList(databaseList)
                                .tableList(tableList)
                                .username(username)
                                .password(password)
                                .serverTimeZone(serverTimeZone)
                                .includeSchemaChanges(includeSchemaChanges)
                                .deserializer(deserializationSchema)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        getName())
                .setParallelism(getParallelism());
    }
}
