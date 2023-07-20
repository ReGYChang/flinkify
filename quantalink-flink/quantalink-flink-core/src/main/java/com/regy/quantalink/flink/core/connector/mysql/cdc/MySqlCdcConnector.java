package com.regy.quantalink.flink.core.connector.mysql.cdc;


import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.mysql.config.MySqlOptions;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public class MySqlCdcConnector extends SourceConnector<String> {

    private final String hostname;
    private final Integer port;
    private final String databaseList;
    private final String tableList;
    private final String username;
    private final String password;
    private final String serverTimeZone;
    private final boolean includeSchema;
    private final boolean includeSchemaChanges;

    public MySqlCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.hostname = config.getNotNull(MySqlOptions.HOSTNAME, "MySQL source CDC connector hostname must not be null");
        this.port = config.getNotNull(MySqlOptions.PORT, "MySQL source CDC connector port must not be null");
        this.databaseList = config.get(MySqlOptions.DATABASE_LIST);
        this.tableList = config.getNotNull(MySqlOptions.TABLE_LIST, "MySQL source CDC connector table list must not be null");
        this.username = config.getNotNull(MySqlOptions.USERNAME, "MySQL source CDC connector username must not be null");
        this.password = config.getNotNull(MySqlOptions.PASSWORD, "MySQL source CDC connector password must not be null");
        this.serverTimeZone = config.get(MySqlOptions.SERVER_TIME_ZONE);
        this.includeSchema = config.get(MySqlOptions.INCLUDE_SCHEMA);
        this.includeSchemaChanges = config.get(MySqlOptions.INCLUDE_SCHEMA_CHANGES);
    }

    @Override
    public DataStreamSource<String> getSourceDataStream() throws FlinkException {
        return getEnv().fromSource(
                        MySqlSource.<String>builder()
                                .hostname(hostname)
                                .port(port)
                                .databaseList(databaseList)
                                .tableList(tableList)
                                .username(username)
                                .password(password)
                                .serverTimeZone(serverTimeZone)
                                .includeSchemaChanges(includeSchemaChanges)
                                .deserializer(new JsonDebeziumDeserializationSchema(includeSchema))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        getName())
                .setParallelism(getParallelism());
    }
}
