package io.github.regychang.flinkify.flink.core.connector.sqlserver.config;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.ConfigOptions;
import io.github.regychang.flinkify.common.config.Configuration;
import java.util.List;
import org.apache.flink.cdc.connectors.base.options.StartupMode;

public interface SqlServerOptions {

    ConfigOption<List<Configuration>> CONNECTORS =
            ConfigOptions.key("sqlserver")
                    .configType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("The connector list of SQL Server.");

    ConfigOption<List<Configuration>> CDC =
            ConfigOptions.key("sqlserver-cdc")
                    .configType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("The CDC connector configuration for SQL Server.");

    ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the SQL Server database.");

    ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(1433)
                    .withDescription("Integer port number of the SQL Server database.");

    ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to use when connecting to the SQL Server database.");

    ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the SQL Server database.");

    ConfigOption<List<String>> DATABASE_LIST =
            ConfigOptions.key("database-list")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Database name of the SQL Server database to monitor.");

    ConfigOption<List<String>> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Table name of the SQL Server database to monitor, e.g.: \"db1.table1\"");

    ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "The session time zone in database server, e.g. \"Asia/Taipei\"");

    ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, "
                                    + "the meta will be divided into multiple groups.");

    ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withDescription(
                            "The lower bound of chunk key distribution factor. "
                                    + "The distribution factor is used to determine whether the table is evenly distribution or not.");

    ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withDescription(
                            "The upper bound of chunk key distribution factor. "
                                    + "The distribution factor is used to determine whether the table is evenly distribution or not.");

    ConfigOption<Configuration> DEBEZIUM_PROPERTIES =
            ConfigOptions.key("debezium")
                    .configType()
                    .defaultValue(new Configuration())
                    .withDescription(
                            "Pass-through Debezium's properties to Debezium Embedded Engine "
                                    + "which is used to capture data changes from SQL Server.");

    ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase.");

    ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of table snapshot, captured tables are split into multiple chunks "
                                    + "by a chunk key when read the snapshot of table. By default, the chunk key is the first "
                                    + "column of the primary key.");

    ConfigOption<StartupMode> STARTUP_MODE =
            ConfigOptions.key("startup-mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription("Startup mode for the connector.");
}
