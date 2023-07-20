package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.SinkConnectorOptions;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

/**
 * @author regy
 */
public abstract class SinkConnector<IN, OUT> extends Connector implements Serializable {

    private SerializationAdapter<OUT, ?> serializationAdapter;
    private OutputTag<IN> outputTag;
    private FlatMapFunction<IN, OUT> transformFunc;
    private TypeInformation<IN> inputType;
    private TypeInformation<OUT> outputType;

    @SuppressWarnings("unchecked")
    public SinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.inputType = (TypeInformation<IN>) config.get(SinkConnectorOptions.INPUT_DATA_TYPE);
        this.outputType = (TypeInformation<OUT>) config.get(SinkConnectorOptions.OUTPUT_DATA_TYPE);
        if (inputType == null && outputType == null) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Input type information & output type information of sink connector '%s' must not be null at one time", getName()));
        } else if (inputType == null) {
            inputType = (TypeInformation<IN>) outputType;
        } else if (outputType == null) {
            outputType = (TypeInformation<OUT>) inputType;
        }
    }

    public abstract DataStreamSink<OUT> createSinkDataStream(DataStream<OUT> stream);

    @SuppressWarnings("unchecked")
    public DataStreamSink<OUT> getSinkDataStream(DataStream<IN> stream) {
        try {
            return createSinkDataStream(
                    inputType.equals(outputType)
                            ? (DataStream<OUT>) mapStream(stream)
                            : mapStream(stream).flatMap(transformFunc).returns(TypeInformation.convertToFlinkType(getOutputType())));
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("Invalid output type of data stream for sink connector `%s`. Please assign a transform function", getName()));
        } catch (NullPointerException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("The transform function for the sink connector '%s' cannot not be null when the input/output type('%s'/'%s') are different", getName(), inputType, outputType));
        }
    }

    private DataStream<IN> mapStream(DataStream<IN> stream) {
        return (outputTag != null && stream instanceof SingleOutputStreamOperator) ?
                ((SingleOutputStreamOperator<IN>) stream).getSideOutput(outputTag) : stream;
    }

    public SinkConnector<IN, OUT> withSerializationAdapter(SerializationAdapter<OUT, ?> serializationAdapter) {
        this.serializationAdapter = serializationAdapter;
        return this;
    }

    public void setOutputTag(OutputTag<IN> outputTag) {
        this.outputTag = outputTag;
    }

    public SinkConnector<IN, OUT> withTransformFunc(FlatMapFunction<IN, OUT> transformFunc) {
        this.transformFunc = transformFunc;
        return this;
    }

    public SerializationAdapter<OUT, ?> getSerializationAdapter() {
        return serializationAdapter;
    }

    public OutputTag<IN> getOutputTag() {
        return outputTag;
    }

    public TypeInformation<IN> getInputType() {
        return inputType;
    }

    public TypeInformation<OUT> getOutputType() {
        return outputType;
    }
}