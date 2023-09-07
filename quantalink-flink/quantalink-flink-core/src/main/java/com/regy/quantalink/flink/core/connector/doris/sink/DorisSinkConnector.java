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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
        dorisOptions = createDorisOptions(config);
        execOptions = createExecOptions(config);

        if (!getJsonFormat().equals(JsonFormat.DEBEZIUM_JSON)) {
            fields = config.getNotNull(DorisOptions.FIELDS);
            types = config.getNotNull(DorisOptions.TYPES);
        } else {
            fields = Collections.emptyList();
            types = Collections.emptyList();
        }
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return properties;
    }

    private org.apache.doris.flink.cfg.DorisOptions createDorisOptions(Configuration config) {
        return org.apache.doris.flink.cfg.DorisOptions.builder()
                .setFenodes(config.getNotNull(DorisOptions.FE_NODES))
                .setTableIdentifier(String.join(".", config.getNotNull(DorisOptions.DATABASE), config.getNotNull(DorisOptions.TABLE)))
                .setUsername(config.getNotNull(DorisOptions.USERNAME))
                .setPassword(config.getNotNull(DorisOptions.PASSWORD))
                .build();
    }

    private DorisExecutionOptions createExecOptions(Configuration config) {
        return DorisExecutionOptions.builder()
                .setLabelPrefix(
                        String.join("-",
                                config.get(DorisOptions.LABEL_PREFIX),
                                config.getNotNull(DorisOptions.DATABASE),
                                config.getNotNull(DorisOptions.TABLE)))
                .setStreamLoadProp(createProperties())
                .build();
    }

    @Override
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        return Optional.of(applyDorisSink(stream.getType().getTypeClass()))
                .map(stream::sinkTo)
                .orElseThrow(
                        () ->
                                new FlinkException(
                                        ErrCode.STREAMING_CONNECTOR_FAILED,
                                        String.format("Could not get sink from stream '%s' to doris connector '%s': ", stream, getName())));
    }

    @SuppressWarnings("unchecked")
    private DorisSink<T> applyDorisSink(Class<T> clazz) {
        DorisSink.Builder<T> builder = initializeBuilder();
        return Optional.of(clazz)
                .map(this::selectSerializerForClass)
                .map(
                        serializer ->
                                builder.setSerializer((DorisRecordSerializer<T>) serializer).build())
                .orElseThrow(
                        () ->
                                new FlinkException(
                                        ErrCode.STREAMING_CONNECTOR_FAILED,
                                        "Could not initialize doris sink, doris sink stream type must be `String` or `RowData`"));
    }

    private DorisSink.Builder<T> initializeBuilder() {
        return DorisSink.<T>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execOptions)
                .setDorisOptions(dorisOptions);
    }

    private DorisRecordSerializer<?> selectSerializerForClass(Class<T> clazz) {
        if (getJsonFormat().equals(JsonFormat.DEBEZIUM_JSON)) {
            return JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
        }
        return clazz.equals(RowData.class) ?
                RowDataSerializer.builder()
                        .setType("json")
                        .setFieldNames(fields.toArray(new String[0]))
                        .setFieldType(types.toArray(new DataType[0])).build() :
                new SimpleStringSerializer();
    }
}
