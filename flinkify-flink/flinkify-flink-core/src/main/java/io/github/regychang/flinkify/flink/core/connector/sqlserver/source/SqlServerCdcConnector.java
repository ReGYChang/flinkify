package io.github.regychang.flinkify.flink.core.connector.sqlserver.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ConfigurationException;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;
import io.github.regychang.flinkify.flink.core.connector.sqlserver.config.SqlServerOptions;
import io.github.regychang.flinkify.flink.core.utils.debezium.DeserializationUtils;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class SqlServerCdcConnector<T> extends SourceConnector<T> {

    private final String hostname;

    private final int port;

    private final List<String> databaseList;

    private final List<String> tableList;

    private final String username;

    private final String password;

    private final String serverTimeZone;

    private final int chunkMetaGroupSize;

    private final double distributionFactorLowerBound;

    private final double distributionFactorUpperBound;

    private final StartupMode startupMode;

    private final Properties debeziumProperties;

    private final boolean closeIdleReaders;

    private final String chunkKeyColumn;

    public SqlServerCdcConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.hostname =
                config.getNotNull(
                        SqlServerOptions.HOSTNAME, "SQL Server hostname must be specified");
        this.port = config.get(SqlServerOptions.PORT);
        this.databaseList =
                config.getNotNull(
                        SqlServerOptions.DATABASE_LIST,
                        "SQL Server database list must be specified");
        this.username =
                config.getNotNull(
                        SqlServerOptions.USERNAME, "SQL Server username must be specified");
        this.password =
                config.getNotNull(
                        SqlServerOptions.PASSWORD, "SQL Server password must be specified");
        this.tableList =
                config.getNotNull(
                        SqlServerOptions.TABLE_LIST,
                        "Table list must be specified (format: [schema.table])");
        this.serverTimeZone = config.get(SqlServerOptions.SERVER_TIME_ZONE);
        this.chunkMetaGroupSize = config.get(SqlServerOptions.CHUNK_META_GROUP_SIZE);
        this.distributionFactorLowerBound =
                config.get(SqlServerOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        this.distributionFactorUpperBound =
                config.get(SqlServerOptions.CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        this.startupMode = config.get(SqlServerOptions.STARTUP_MODE);
        this.debeziumProperties =
                convertToProperties(config.get(SqlServerOptions.DEBEZIUM_PROPERTIES));
        this.closeIdleReaders =
                config.get(SqlServerOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        this.chunkKeyColumn =
                config.get(SqlServerOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);

        validateConfigurations();
    }

    private void validateConfigurations() {
        tableList.forEach(
                tableName -> {
                    if (!isValidTableName(tableName)) {
                        throw new ConfigurationException(
                                ErrCode.PARSING_CONFIG_FAILED,
                                String.format(
                                        "Invalid table name format: %s. Expected format: 'schema.table'",
                                        tableName));
                    }
                });
        if (port <= 0 || port > 65535) {
            throw new ConfigurationException(
                    ErrCode.PARSING_CONFIG_FAILED,
                    String.format("Port must be between 1 and 65535, got: %d", port));
        }
        if (distributionFactorLowerBound >= distributionFactorUpperBound) {
            throw new ConfigurationException(
                    ErrCode.PARSING_CONFIG_FAILED,
                    String.format(
                            "Distribution factor lower bound (%f) must be less than upper bound (%f)",
                            distributionFactorLowerBound, distributionFactorUpperBound));
        }
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        try {
            DeserializationAdapter<T, ?> deserializationAdapter = getDeserializationAdapter();
            DebeziumDeserializationSchema<T> deserializationSchema =
                    DeserializationUtils.extractDeserializationSchema(deserializationAdapter);
            SqlServerIncrementalSource<T> sqlServerSource = buildSource(deserializationSchema);
            StreamExecutionEnvironment env = getEnv();
            int parallelism = getParallelism();

            return env.fromSource(
                            sqlServerSource,
                            WatermarkStrategy.noWatermarks(),
                            "SQL Server CDC Source")
                    .setParallelism(parallelism);
        } catch (Exception e) {
            log.error("Failed to create SQL Server CDC source", e);
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    "Failed to create SQL Server CDC source",
                    e);
        }
    }

    private SqlServerIncrementalSource<T> buildSource(
            DebeziumDeserializationSchema<T> deserializationSchema) {
        return SqlServerIncrementalSource.<T>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(databaseList.toArray(new String[0]))
                .tableList(tableList.toArray(new String[0]))
                .serverTimeZone(serverTimeZone)
                .startupOptions(convertToStartupOptions(startupMode))
                .debeziumProperties(debeziumProperties)
                .distributionFactorLower(distributionFactorLowerBound)
                .distributionFactorUpper(distributionFactorUpperBound)
                .splitMetaGroupSize(chunkMetaGroupSize)
                .chunkKeyColumn(chunkKeyColumn)
                .closeIdleReaders(closeIdleReaders)
                .deserializer(deserializationSchema)
                .build();
    }

    private Properties convertToProperties(Configuration config) {
        Properties properties = new Properties();
        properties.putAll(config.toMap());
        return properties;
    }

    private StartupOptions convertToStartupOptions(StartupMode startupMode) {
        switch (startupMode) {
            case INITIAL:
                return StartupOptions.initial();
            case LATEST_OFFSET:
                return StartupOptions.latest();
            case EARLIEST_OFFSET:
                return StartupOptions.earliest();
            default:
                log.warn("Unsupported startup mode: {}. Using INITIAL as default.", startupMode);
                return StartupOptions.initial();
        }
    }

    private static boolean isValidTableName(String tableName) {
        return tableName != null && tableName.matches("\\w+\\.\\w+");
    }
}
