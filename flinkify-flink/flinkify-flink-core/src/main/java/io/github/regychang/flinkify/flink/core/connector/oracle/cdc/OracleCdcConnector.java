package io.github.regychang.flinkify.flink.core.connector.oracle.cdc;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.oracle.config.OracleOptions;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import io.github.regychang.flinkify.flink.core.utils.debezium.DeserializationUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class OracleCdcConnector<T> extends SourceConnector<T> {

    private final String username;

    private final String password;

    private final String database;

    private final String[] schemaList;

    private final String[] tableList;

    private final String hostname;

    private final Integer port;

    private final String url;

    private final Properties debeziumProperties;

    private final StartupOptions startupOptions;

    public OracleCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.username =
                config.getNotNull(
                        OracleOptions.USERNAME, "Oracle CDC connector username must not be null");
        this.password =
                config.getNotNull(
                        OracleOptions.PASSWORD, "Oracle CDC connector password must not be null");
        this.database =
                config.getNotNull(
                        OracleOptions.DATABASE, "Oracle CDC connector database must not be null");
        this.schemaList =
                config.getNotNull(
                                OracleOptions.SCHEMA_LIST,
                                "Oracle CDC connector schema-list must not be null")
                        .toArray(new String[0]);
        this.tableList =
                config.getNotNull(
                                OracleOptions.TABLE_LIST,
                                "Oracle CDC connector table-list must not be null")
                        .toArray(new String[0]);
        this.debeziumProperties = config.get(OracleOptions.DEBEZIUM_PROPERTIES).toProperties();
        this.hostname = config.get(OracleOptions.HOSTNAME);
        this.port = config.get(OracleOptions.PORT);
        this.url = config.get(OracleOptions.URL);
        this.startupOptions = getStartupOptions(config);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        DebeziumDeserializationSchema<T> deserializationSchema =
                DeserializationUtils.extractDeserializationSchema(getDeserializationAdapter());

        return getEnv().addSource(
                        OracleSource.<T>builder()
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
                                .deserializer(deserializationSchema)
                                .build(),
                        getName())
                .setParallelism(1);
    }

    private StartupOptions getStartupOptions(Configuration config) {
        switch (config.get(OracleOptions.STARTUP_MODE)) {
            case INITIAL:
                return StartupOptions.initial();
            case LATEST_OFFSET:
                return StartupOptions.latest();
            case EARLIEST_OFFSET:
                return StartupOptions.earliest();
            case SPECIFIC_OFFSETS:
                return StartupOptions.specificOffset(
                        config.getNotNull(
                                OracleOptions.SPECIFIC_OFFSET_FILE,
                                "Oracle CDC connector specific-offset-file must not be null when startup mode is [SPECIFIC_OFFSETS]"),
                        config.getNotNull(
                                OracleOptions.SPECIFIC_OFFSET_POS,
                                "Oracle CDC connector specific-offset-pos must not be null when startup mode is [SPECIFIC_OFFSETS]"));
            case TIMESTAMP:
                return StartupOptions.timestamp(
                        config.getNotNull(
                                OracleOptions.STARTUP_TIMESTAMP_MS,
                                "Oracle CDC connector startup-timestamp-ms must not be null when startup mode is [TIMESTAMP]"));
            default:
                throw new IllegalArgumentException(
                        "Unsupported startup mode: " + config.get(OracleOptions.STARTUP_MODE));
        }
    }
}
