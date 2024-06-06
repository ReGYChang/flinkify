package io.github.regychang.flinkify.flink.core.connector.doris.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.RowDataDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DorisSourceConnector extends SourceConnector<RowData> {

    private final List<String> fields;

    private final List<DataType> types;

    private final DorisOptions dorisOptions;

    private final DorisReadOptions dorisReadOptions;

    public DorisSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        String feNodes = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.FE_NODES);
        String database = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.DATABASE);
        String table = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.TABLE);
        String username = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.USERNAME);
        String password = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.PASSWORD);
        String readFields = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.READ_FIELDS);
        String filterQuery = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.FILTER_QUERY);
        Integer requestTabletSize = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_TABLET_SIZE);
        Integer requestConnectTimeoutMs = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_CONNECT_TIMEOUT_MS);
        Integer requestReadTimeoutMs = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_READ_TIMEOUT_MS);
        Integer requestQueryTimeoutS = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_QUERY_TIMEOUT_S);
        Integer requestRetries = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_RETRIES);
        Integer requestBatchSize = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.REQUEST_BATCH_SIZE);
        Long execMemLimit = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.EXEC_MEM_LIMIT);
        Integer deserializeQueueSize = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.DESERIALIZE_QUEUE_SIZE);
        Boolean deserializeArrowAsync = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.DESERIALIZE_ARROW_ASYNC);
        Boolean useOldApi = config.get(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.USE_OLD_API);
        this.fields = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.FIELDS);
        this.types = config.getNotNull(io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions.TYPES);
        this.dorisOptions = org.apache.doris.flink.cfg.DorisOptions.builder().setFenodes(feNodes).setTableIdentifier(String.join(".", database, table)).setUsername(username).setPassword(password).build();
        this.dorisReadOptions = DorisReadOptions.builder()
                .setReadFields(readFields)
                .setFilterQuery(filterQuery)
                .setRequestTabletSize(requestTabletSize)
                .setRequestConnectTimeoutMs(requestConnectTimeoutMs)
                .setRequestReadTimeoutMs(requestReadTimeoutMs)
                .setRequestQueryTimeoutS(requestQueryTimeoutS)
                .setRequestRetries(requestRetries)
                .setRequestBatchSize(requestBatchSize)
                .setExecMemLimit(execMemLimit)
                .setDeserializeQueueSize(deserializeQueueSize)
                .setDeserializeArrowAsync(deserializeArrowAsync)
                .setUseOldApi(useOldApi).build();
    }

    @Override
    public DataStreamSource<RowData> getSourceDataStream() throws FlinkException {
        LogicalType[] logicalTypes = TypeConversions.fromDataToLogicalType(types.toArray(new DataType[0]));
        List<RowType.RowField> rowFields =
                IntStream.range(0, logicalTypes.length)
                        .mapToObj(i -> new RowType.RowField(fields.get(i), logicalTypes[i]))
                        .collect(Collectors.toList());

        DorisSource<RowData> dorisSource =
                DorisSourceBuilder.<RowData>builder()
                        .setDorisOptions(dorisOptions)
                        .setDorisReadOptions(dorisReadOptions)
                        .setDeserializer(new RowDataDeserializationSchema(new RowType(rowFields)))
                        .build();

        return getEnv().fromSource(dorisSource, getWatermarkStrategy(), getName());
    }
}
