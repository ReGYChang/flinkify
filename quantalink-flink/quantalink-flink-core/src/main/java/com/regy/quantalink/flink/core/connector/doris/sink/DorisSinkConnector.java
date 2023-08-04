package com.regy.quantalink.flink.core.connector.doris.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.doris.config.DorisOptions;
import com.regy.quantalink.flink.core.utils.JsonFormat;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Properties;

/**
 * @author regy
 */
public class DorisSinkConnector<T> extends SinkConnector<T, T> {

    private final List<String> fields;
    private final List<DataType> types;
    private final org.apache.doris.flink.cfg.DorisOptions dorisOptions;
    private final DorisExecutionOptions execOptions;

    public DorisSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        String feNodes = config.getNotNull(DorisOptions.FE_NODES, String.format("Doris sink connector '%s' fe-nodes must not be null", getName()));
        String database = config.getNotNull(DorisOptions.DATABASE, String.format("Doris sink connector '%s' database must not be null", getName()));
        String table = config.getNotNull(DorisOptions.TABLE, String.format("Doris sink connector '%s' table must not be null", getName()));
        String username = config.getNotNull(DorisOptions.USERNAME, String.format("Doris sink connector '%s' username must not be null", getName()));
        String password = config.getNotNull(DorisOptions.PASSWORD, String.format("Doris sink connector '%s' password must not be null", getName()));
        String labelPrefix = config.getNotNull(DorisOptions.LABEL_PREFIX, String.format("Doris sink connector '%s' label-prefix must not be null", getName()));
        this.fields = config.getNotNull(DorisOptions.FIELDS, String.format("Doris sink connector '%s' fields must not be null", getName()));
        this.types = config.getNotNull(DorisOptions.TYPES, String.format("Doris sink connector '%s' types must not be null", getName()));
        this.dorisOptions = org.apache.doris.flink.cfg.DorisOptions.builder().setFenodes(feNodes).setTableIdentifier(String.join(".", database, table)).setUsername(username).setPassword(password).build();
        this.execOptions = DorisExecutionOptions.builder().setLabelPrefix(String.join("-", labelPrefix, database, table)).setStreamLoadProp(properties).build();
    }

    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        DorisSink<T> dorisSink = applyDorisSink(stream.getType().getTypeClass());
        try {
            return stream.sinkTo(dorisSink);
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Could not get sink from stream '%s' to doris connector '%s': ", stream, getName()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private DorisSink<T> applyDorisSink(Class<T> clazz) {
        try {
            DorisSink.Builder<T> builder = DorisSink.<T>builder()
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(execOptions)
                    .setDorisOptions(dorisOptions);

            if (getJsonFormat().equals(JsonFormat.DEBEZIUM_JSON)) {
                DorisRecordSerializer<?> serializer =
                        clazz.equals(RowData.class)
                                ?
                                RowDataSerializer.builder()
                                        .setType("json")
                                        .setFieldNames(fields.toArray(new String[0]))
                                        .setFieldType(types.toArray(new DataType[0])).build()
                                : new SimpleStringSerializer();

                builder.setSerializer((DorisRecordSerializer<T>) serializer);
            } else {
                builder.setSerializer((DorisRecordSerializer<T>) JsonDebeziumSchemaSerializer.builder()
                        .setDorisOptions(dorisOptions).build());
            }
            return builder.build();
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, "Could not initialize doris sink, doris sink stream type must be `String` or `RowData`", e);
        }
    }
}
