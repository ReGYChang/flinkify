package com.regy.quantalink.flink.core.streaming;

import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Optional;

/**
 * @author regy
 */
public class FlinkDataStream<T> {

    private final DataStream<T> dataStream;
    private final FlinkStreamingContext context;
    private final TypeInformation<T> typeInformation;

    public DataStream<T> get() {
        return this.dataStream;
    }

    public static <T> FlinkDataStream<T> ofDataStream(DataStreamSource<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public static <T> FlinkDataStream<T> ofDataStream(SingleOutputStreamOperator<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public <T2> FlinkDataStream<T> sideSink(com.regy.quantalink.common.type.TypeInformation<T2> typeInfo) {
        if (this.dataStream instanceof SingleOutputStreamOperator) {
            SinkConnector<T2> sinkConnector = this.context.getSinkConnector(typeInfo);
            sinkConnector.getSinkDataStream(((SingleOutputStreamOperator<T>) this.dataStream).getSideOutput(sinkConnector.getOutputTag()));
            return this;
        }
        throw new FlinkException(ErrCode.STREAMING_EXECUTION_FAILED, String.format("Could not sink from side output stream with type: '%s'", typeInfo));
    }

    public <T2> FlinkDataStream<T> sidePrint(com.regy.quantalink.common.type.TypeInformation<T2> typeInfo) {
        if (this.dataStream instanceof SingleOutputStreamOperator) {
            SinkConnector<T2> sinkConnector = this.context.getSinkConnector(typeInfo);
            ((SingleOutputStreamOperator<T>) this.dataStream).getSideOutput(sinkConnector.getOutputTag()).print();
            return this;
        }
        throw new FlinkException(ErrCode.STREAMING_EXECUTION_FAILED, String.format("Could not sink from side output stream with type: '%s'", typeInfo));
    }

    public void sink() {
        com.regy.quantalink.common.type.TypeInformation<T> typeInfo = com.regy.quantalink.common.type.TypeInformation.get(this.typeInformation);
        SinkConnector<T> sinkConnector = this.context.getSinkConnector(typeInfo);
        sinkConnector.getSinkDataStream(this.dataStream);
    }

    private FlinkDataStream(DataStream<T> dataStream, FlinkStreamingContext context) {
        this.dataStream = Optional.ofNullable(dataStream).orElseThrow(() -> new FlinkException(ErrCode.STREAMING_EXECUTION_FAILED, "Initialize Flink data stream failed, data stream must not be null"));
        this.context = context;
        this.typeInformation = dataStream.getType();
    }
}
