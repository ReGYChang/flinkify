package io.github.regychang.flinkify.flink.core.connector.doris.sink;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SinkConnector;
import io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions;
import io.github.regychang.flinkify.flink.core.connector.doris.func.ObjectStringTransformFunc;
import io.github.regychang.flinkify.flink.core.utils.JsonFormat;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class DorisSinkConnector<IN, OUT> extends SinkConnector<IN, OUT> {

    private final List<String> fields;

    private final List<DataType> types;

    private final org.apache.doris.flink.cfg.DorisOptions dorisOptions;

    private final DorisExecutionOptions execOptions;

    private final DorisSinkType dorisSinkType;

    public DorisSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.dorisSinkType = determineSinkType();
        this.dorisOptions = createDorisOptions(config);
        this.execOptions = createExecOptions(config);
        this.fields = (dorisSinkType == DorisSinkType.ROW) ? config.getNotNull(DorisOptions.FIELDS) : Collections.emptyList();
        this.types = (dorisSinkType == DorisSinkType.ROW) ? config.getNotNull(DorisOptions.TYPES) : Collections.emptyList();
    }

    @Override
    public DataStreamSink<OUT> createSinkDataStream(DataStream<OUT> stream) {
        return Optional.of(applyDorisSink(stream.getType().getTypeClass()))
                .map(stream::sinkTo)
                .orElseThrow(
                        () ->
                                new FlinkException(
                                        ErrCode.STREAMING_CONNECTOR_FAILED,
                                        String.format(
                                                "Could not get sink from stream '%s' to doris connector '%s': ",
                                                stream,
                                                getName())));
    }

    @SuppressWarnings("unchecked")
    private DorisSinkType determineSinkType() {
        if (getJsonFormat().equals(JsonFormat.DEBEZIUM_JSON)) {
            return DorisSinkType.DEBEZIUM_STRING;
        } else if (super.getOutputType().getRawType().equals(String.class)) {
            super.setTransformFunc((FlatMapFunction<IN, OUT>) new ObjectStringTransformFunc<IN>());
            return DorisSinkType.STRING;
        } else {
            return DorisSinkType.ROW;
        }
    }

    private Properties createProperties() {
        return Optional.of(new Properties())
                .filter(props -> dorisSinkType != DorisSinkType.STRING)
                .map(
                        props -> {
                            props.setProperty("format", "json");
                            props.setProperty("read_json_by_line", "true");
                            return props;
                        })
                .orElse(new Properties());
    }

    private org.apache.doris.flink.cfg.DorisOptions createDorisOptions(Configuration config) {
        return org.apache.doris.flink.cfg.DorisOptions.builder()
                .setFenodes(config.getNotNull(DorisOptions.FE_NODES))
                .setTableIdentifier(
                        String.join(".",
                                config.getNotNull(DorisOptions.DATABASE),
                                config.getNotNull(DorisOptions.TABLE)))
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

    @SuppressWarnings("unchecked")
    private DorisSink<OUT> applyDorisSink(Class<OUT> clazz) {
        DorisSink.Builder<OUT> builder = initializeBuilder();
        return Optional.of(clazz)
                .map(this::selectSerializerForClass)
                .map(
                        serializer ->
                                builder.setSerializer((DorisRecordSerializer<OUT>) serializer).build())
                .orElseThrow(
                        () ->
                                new FlinkException(
                                        ErrCode.STREAMING_CONNECTOR_FAILED,
                                        "Could not initialize doris sink," +
                                                " doris sink stream type must be `String` or `RowData`"));
    }

    private DorisSink.Builder<OUT> initializeBuilder() {
        return DorisSink.<OUT>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(execOptions)
                .setDorisOptions(dorisOptions);
    }

    private DorisRecordSerializer<?> selectSerializerForClass(Class<OUT> clazz) {
        if (dorisSinkType == DorisSinkType.DEBEZIUM_STRING) {
            return JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build();
        }
        if (dorisSinkType == DorisSinkType.STRING) {
            return new SimpleStringSerializer();
        }
        return RowDataSerializer.builder()
                .setType("json")
                .setFieldNames(fields.toArray(new String[0]))
                .setFieldType(types.toArray(new DataType[0]))
                .build();
    }

    private enum DorisSinkType {
        DEBEZIUM_STRING, STRING, ROW
    }
}
