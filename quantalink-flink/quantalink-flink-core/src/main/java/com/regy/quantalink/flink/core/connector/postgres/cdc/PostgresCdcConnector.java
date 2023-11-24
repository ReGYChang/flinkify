package com.regy.quantalink.flink.core.connector.postgres.cdc;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.postgres.config.PostgresOptions;

import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class PostgresCdcConnector extends SourceConnector<String> {

    private final String hostname;
    private final Integer port;
    private final String username;
    private final String password;
    private final String database;
    private final String[] schemaList;
    private final String[] tableList;
    private final String slotName;
    private final String decodingPluginName;
    private final Boolean includeSchemaChange;
    private final Integer splitSize;
    private final Properties debeziumProperties;
    private final StartupOptions startupOptions;

    public PostgresCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.hostname = config.getNotNull(PostgresOptions.HOSTNAME);
        this.port = config.get(PostgresOptions.PORT);
        this.username = config.getNotNull(PostgresOptions.USERNAME);
        this.password = config.getNotNull(PostgresOptions.PASSWORD);
        this.database = config.getNotNull(PostgresOptions.DATABASE);
        this.schemaList = config.getNotNull(PostgresOptions.SCHEMA_LIST).toArray(new String[0]);
        this.tableList = config.getNotNull(PostgresOptions.TABLE_LIST).toArray(new String[0]);
        this.slotName = config.getNotNull(PostgresOptions.SLOT_NAME);
        this.decodingPluginName = config.getNotNull(PostgresOptions.DECODING_PLUGIN_NAME);
        this.includeSchemaChange = config.get(PostgresOptions.INCLUDE_SCHEMA_CHANGE);
        this.splitSize = config.get(PostgresOptions.SPLIT_SIZE);
        this.debeziumProperties = config.get(PostgresOptions.DEBEZIUM_PROPERTIES).toProperties();
        if (config.get(PostgresOptions.STARTUP_MODE) == StartupMode.LATEST_OFFSET) {
            this.startupOptions = StartupOptions.latest();
        } else {
            this.startupOptions = StartupOptions.initial();
        }
    }

    @Override
    public DataStreamSource<String> getSourceDataStream() throws FlinkException {
        DebeziumDeserializationSchema<String> deserializer = new JsonDebeziumDeserializationSchema();
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(hostname)
                        .port(port)
                        .database(database)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .username(username)
                        .password(password)
                        .slotName(slotName)
                        .decodingPluginName(decodingPluginName)
                        .deserializer(deserializer)
                        .includeSchemaChanges(includeSchemaChange)
                        .splitSize(splitSize)
                        .startupOptions(startupOptions)
                        .debeziumProperties(debeziumProperties)
                        .build();

        return getEnv().fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        getName())
                .setParallelism(getParallelism());
    }
}
