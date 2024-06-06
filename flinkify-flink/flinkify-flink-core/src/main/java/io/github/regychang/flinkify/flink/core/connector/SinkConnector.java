package io.github.regychang.flinkify.flink.core.connector;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.config.SinkConnectorOptions;
import io.github.regychang.flinkify.flink.core.connector.serialization.SerializationAdapter;
import io.github.regychang.flinkify.flink.core.utils.JsonFormat;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Optional;

@Getter
@Setter
public abstract class SinkConnector<IN, OUT> extends Connector implements Serializable {

    private final TypeInformation<IN> inputType;

    private final TypeInformation<OUT> outputType;

    private final JsonFormat jsonFormat;

    private OutputTag<IN> outputTag;

    private SerializationAdapter<OUT, ?> serializationAdapter;

    private FlatMapFunction<IN, OUT> transformFunc;

    @SuppressWarnings("unchecked")
    public SinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);

        this.jsonFormat = config.get(SinkConnectorOptions.JSON_FORMAT);
        TypeInformation<IN> initialInputType = (TypeInformation<IN>) config.get(SinkConnectorOptions.INPUT_DATA_TYPE);
        TypeInformation<OUT> initialOutputType = (TypeInformation<OUT>) config.get(SinkConnectorOptions.OUTPUT_DATA_TYPE);

        if (initialInputType == null && initialOutputType == null) {
            throw new FlinkException(
                    ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format(
                            "Input type information & output type information of sink connector" +
                                    " '%s' must not be null at one time", getName()));
        }

        this.inputType = Optional.ofNullable(initialInputType).orElse((TypeInformation<IN>) initialOutputType);
        this.outputType = Optional.ofNullable(initialOutputType).orElse((TypeInformation<OUT>) initialInputType);
    }

    public abstract DataStreamSink<OUT> createSinkDataStream(DataStream<OUT> stream);

    @SuppressWarnings("unchecked")
    public DataStreamSink<OUT> getSinkDataStream(DataStream<IN> stream) {
        try {
            return createSinkDataStream(
                    inputType.equals(outputType) ?
                            (DataStream<OUT>) mapStream(stream) :
                            mapStream(stream)
                                    .flatMap(transformFunc)
                                    .returns(TypeInformation.convertToFlinkType(getOutputType())))
                    .setParallelism(getParallelism())
                    .name(getName())
                    .disableChaining();
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("Invalid output type of data stream for sink connector `%s`." +
                            "Please assign a transform function", getName()));
        } catch (NullPointerException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("The transform function for the sink connector '%s' cannot not be null" +
                            " when the input/output type('%s'/'%s') are different", getName(), inputType, outputType));
        }
    }

    private DataStream<IN> mapStream(DataStream<IN> stream) {
        return (
                outputTag != null &&
                        stream instanceof SingleOutputStreamOperator) ?
                ((SingleOutputStreamOperator<IN>) stream).getSideOutput(outputTag) : stream;
    }
}
