package com.regy.quantalink.flink.core.connector.oracle.cdc;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;
import com.regy.quantalink.flink.core.connector.oracle.config.OracleOptions;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author regy
 */
public class OracleCdcConnector extends SourceConnector<String> {

    private final String username;
    private final String password;
    private final String database;
    private final String[] schemaList;
    private final String[] tableList;
    private final String hostname;
    private final Integer port;
    private final String url;
    private StartupOptions startupOptions;
    private final Properties debeziumProperties;

    public OracleCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.username = config.getNotNull(OracleOptions.USERNAME, "Oracle CDC connector hostname must not be null");
        this.password = config.getNotNull(OracleOptions.PASSWORD, "Oracle CDC connector hostname must not be null");
        this.database = config.getNotNull(OracleOptions.DATABASE, "Oracle CDC connector hostname must not be null");
        this.schemaList = config.getNotNull(OracleOptions.SCHEMA_LIST, "Oracle CDC connector hostname must not be null").toArray(new String[0]);
        this.tableList = config.getNotNull(OracleOptions.TABLE_LIST, "Oracle CDC connector hostname must not be null").toArray(new String[0]);
        this.debeziumProperties = config.get(OracleOptions.DEBEZIUM_PROPERTIES).toProperties();
        this.hostname = config.get(OracleOptions.HOSTNAME);
        this.port = config.get(OracleOptions.PORT);
        this.url = config.get(OracleOptions.URL);
        switch (config.get(OracleOptions.STARTUP_MODE)) {
            case INITIAL:
                this.startupOptions = StartupOptions.initial();
                break;
            case LATEST_OFFSET:
                this.startupOptions = StartupOptions.latest();
                break;
            case EARLIEST_OFFSET:
                this.startupOptions = StartupOptions.earliest();
                break;
            case SPECIFIC_OFFSETS:
                this.startupOptions =
                        StartupOptions.specificOffset(
                                config.getNotNull(OracleOptions.SPECIFIC_OFFSET_FILE, "Oracle CDC connector specific-offset-file must not be null when startup mode is [SPECIFIC_OFFSETS]"),
                                config.getNotNull(OracleOptions.SPECIFIC_OFFSET_POS, "Oracle CDC connector specific-offset-pos must not be null when startup mode is [SPECIFIC_OFFSETS]"));
                break;
            case TIMESTAMP:
                this.startupOptions =
                        StartupOptions.timestamp(config.getNotNull(OracleOptions.STARTUP_TIMESTAMP_MS, "Oracle CDC connector startup-timestamp-ms must not be null when startup mode is [TIMESTAMP]"));
                break;
        }

    }

    @Override
    public DataStreamSource<String> getSourceDataStream() throws FlinkException {
        return getEnv().addSource(
                        OracleSource.<String>builder()
                                .url(url)
                                .hostname(hostname)
                                .port(port)
                                .database(database)
                                .schemaList(schemaList)
                                .tableList(tableList)
                                .username(username)
                                .password(password)
                                .startupOptions(startupOptions)
                                .debeziumProperties(debeziumProperties)
                                .deserializer(new JsonDebeziumDeserializationSchema()).build(),
                        getName())
                .setParallelism(1);
    }
}
