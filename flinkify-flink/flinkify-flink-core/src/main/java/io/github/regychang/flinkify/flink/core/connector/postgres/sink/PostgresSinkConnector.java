package io.github.regychang.flinkify.flink.core.connector.postgres.sink;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.flink.core.connector.SinkConnector;
import io.github.regychang.flinkify.flink.core.connector.postgres.config.PostgresOptions;
import io.github.regychang.flinkify.flink.core.utils.jdbc.ColumnInfo;
import io.github.regychang.flinkify.flink.core.utils.jdbc.ColumnUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PostgresSinkConnector<T> extends SinkConnector<T, T> {

    private static final String DRIVER_NAME = "org.postgresql.Driver";

    private static final String URL = "jdbc:postgresql://%s:%d/%s";

    private static final String INSERT_STATEMENT = "INSERT INTO %s.%s (%s) VALUES (%s)";

    private final String hostname;

    private final Integer port;

    private final String username;

    private final String password;

    private final String database;

    private final String schema;

    private final String table;

    private final Integer batchSize;

    private final Long batchIntervalMs;

    private final Integer maxRetries;

    public PostgresSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.hostname = config.getNotNull(PostgresOptions.HOSTNAME);
        this.port = config.getNotNull(PostgresOptions.PORT);
        this.username = config.getNotNull(PostgresOptions.USERNAME);
        this.password = config.getNotNull(PostgresOptions.PASSWORD);
        this.database = config.getNotNull(PostgresOptions.DATABASE);
        this.table = config.getNotNull(PostgresOptions.TABLE);
        this.schema = config.getNotNull(PostgresOptions.SCHEMA);
        this.batchSize = config.getNotNull(PostgresOptions.BATCH_SIZE);
        this.batchIntervalMs = config.getNotNull(PostgresOptions.BATCH_INTERVAL_MS);
        this.maxRetries = config.getNotNull(PostgresOptions.MAX_RETRIES);
    }

    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        JdbcConnectionOptions connectionOptions =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(String.format(URL, hostname, port, database))
                        .withDriverName(DRIVER_NAME)
                        .withUsername(username)
                        .withPassword(password)
                        .build();

        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(maxRetries)
                        .build();

        SinkFunction<T> sink =
                createSink(schema, table, getOutputType().getRawType(), executionOptions, connectionOptions);

        return stream.addSink(sink);
    }

    private SinkFunction<T> createSink(
            String schema,
            String table,
            Class<T> rawType,
            JdbcExecutionOptions executionOptions,
            JdbcConnectionOptions connectionOptions) {

        List<ColumnInfo> columnInfos = ColumnUtils.getColumnInfo(rawType);
        String fields = columnInfos.stream().map(ColumnInfo::getName).collect(Collectors.joining(", "));
        String placeholders = columnInfos.stream().map(f -> "?").collect(Collectors.joining(", "));
        String sql = String.format(INSERT_STATEMENT, schema, table, fields, placeholders);

        return JdbcSink.sink(
                sql,
                (statement, record) -> setStatementFields(statement, rawType, record),
                executionOptions,
                connectionOptions);
    }

    private void setStatementFields(PreparedStatement statement, Class<T> rawType, T record) {
        List<ColumnInfo> columnInfos = ColumnUtils.getColumnInfo(rawType);
        IntStream.range(0, columnInfos.size())
                .forEach(
                        idx -> {
                            ColumnInfo columnInfo = columnInfos.get(idx);
                            try {
                                setStatementField(
                                        statement, idx + 1, columnInfo, record);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        String.format(
                                                "Error setting statement field for column '%s' (index %d) of type %s: %s",
                                                columnInfo.getName(), idx + 1, columnInfo.getType(), e.getMessage()));
                            }
                        });
    }

    private void setStatementField(
            PreparedStatement statement, int index, ColumnInfo columnInfo, T record) throws Exception {
        Object value = getFieldValue(record, columnInfo.getField());
        if (value == null) {
            statement.setNull(index, columnInfo.getType().getVendorTypeNumber());
        } else {
            try {
                switch (columnInfo.getType()) {
                    case VARCHAR:
                    case CHAR:
                    case LONGVARCHAR:
                    case NVARCHAR:
                    case NCHAR:
                    case LONGNVARCHAR:
                        statement.setString(index, value.toString());
                        break;
                    case INTEGER:
                        statement.setInt(index, ((Number) value).intValue());
                        break;
                    case BIGINT:
                        statement.setLong(index, ((Number) value).longValue());
                        break;
                    case FLOAT:
                    case REAL:
                        statement.setFloat(index, ((Number) value).floatValue());
                        break;
                    case DOUBLE:
                        statement.setDouble(index, ((Number) value).doubleValue());
                        break;
                    case DECIMAL:
                    case NUMERIC:
                        if (value instanceof BigDecimal) {
                            statement.setBigDecimal(index, (BigDecimal) value);
                        } else if (value instanceof Number) {
                            statement.setBigDecimal(index, new BigDecimal(value.toString()));
                        } else {
                            throw new IllegalArgumentException("Value is not a Number");
                        }
                        break;
                    case DATE:
                        if (value instanceof LocalDate) {
                            statement.setDate(index, java.sql.Date.valueOf((LocalDate) value));
                        } else if (value instanceof Date) {
                            statement.setDate(index, new java.sql.Date(((Date) value).getTime()));
                        } else {
                            throw new IllegalArgumentException("Unsupported Date type");
                        }
                        break;
                    case TIMESTAMP:
                        if (value instanceof Instant) {
                            statement.setTimestamp(index, java.sql.Timestamp.from((Instant) value));
                        } else if (value instanceof LocalDateTime) {
                            statement.setTimestamp(index, java.sql.Timestamp.valueOf((LocalDateTime) value));
                        } else if (value instanceof Date) {
                            statement.setTimestamp(index, new java.sql.Timestamp(((Date) value).getTime()));
                        } else {
                            throw new IllegalArgumentException("Unsupported Timestamp type");
                        }
                        break;
                    case BOOLEAN:
                        if (value instanceof Boolean) {
                            statement.setBoolean(index, (Boolean) value);
                        } else if (value instanceof Number) {
                            statement.setBoolean(index, ((Number) value).intValue() != 0);
                        } else if (value instanceof String) {
                            statement.setBoolean(index, Boolean.parseBoolean((String) value));
                        } else {
                            throw new IllegalArgumentException("Unsupported Boolean type");
                        }
                        break;
                    case BINARY:
                    case VARBINARY:
                    case LONGVARBINARY:
                        if (value instanceof byte[]) {
                            statement.setBytes(index, (byte[]) value);
                        } else {
                            throw new IllegalArgumentException("Value is not a byte array");
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported JDBC type: " + columnInfo.getType());
                }
            } catch (ClassCastException | IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Data type mismatch for column '%s'. Expected %s, but got %s",
                                columnInfo.getName(), columnInfo.getType(), value.getClass().getSimpleName()), e);
            }
        }
    }

    private Object getFieldValue(T record, Field field) {
        try {
            field.setAccessible(true);
            return field.get(record);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Error getting field value", e);
        }
    }
}
