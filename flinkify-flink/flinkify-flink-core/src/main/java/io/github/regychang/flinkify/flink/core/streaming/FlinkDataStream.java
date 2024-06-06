package io.github.regychang.flinkify.flink.core.streaming;

import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Optional;

public class FlinkDataStream<T> {

    private final DataStream<T> dataStream;

    private final FlinkStreamingContext context;

    private final TypeInformation<T> typeInformation;

    public DataStream<T> get() {
        return this.dataStream;
    }

    public static <T> FlinkDataStream<T> ofDataStream(
            DataStream<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public static <T> FlinkDataStream<T> ofDataStream(
            DataStreamSource<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public static <T> FlinkDataStream<T> ofDataStream(
            SingleOutputStreamOperator<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public static <T> FlinkDataStream<T> ofDataStream(
            SideOutputDataStream<T> stream, FlinkStreamingContext context) throws FlinkException {
        return new FlinkDataStream<>(stream, context);
    }

    public FlinkDataStream<T> sink() {
        io.github.regychang.flinkify.common.type.TypeInformation<T> typeInfo =
                io.github.regychang.flinkify.common.type.TypeInformation.get(this.typeInformation.getTypeClass());
        context.getSinkDataStream(typeInfo, typeInfo, dataStream);
        return this;
    }

    public FlinkDataStream<T> sink(io.github.regychang.flinkify.common.type.TypeInformation<?> outputTypeInfo) {
        context.getSinkDataStream(
                io.github.regychang.flinkify.common.type.TypeInformation.get(
                        typeInformation.getTypeClass()), outputTypeInfo, dataStream);
        return this;
    }

    public FlinkDataStream<T> sink(String sinkConnectorId) {
        this.sink(
                sinkConnectorId,
                io.github.regychang.flinkify.common.type.TypeInformation.get(typeInformation.getTypeClass()));
        return this;
    }

    public FlinkDataStream<T> sink(
            String sinkConnectorId, io.github.regychang.flinkify.common.type.TypeInformation<?> outputTypeInfo) {
        context.getSinkDataStream(
                sinkConnectorId,
                io.github.regychang.flinkify.common.type.TypeInformation.get(
                        typeInformation.getTypeClass()), outputTypeInfo, dataStream);
        return this;
    }

    private FlinkDataStream(DataStream<T> dataStream, FlinkStreamingContext context) {
        this.dataStream =
                Optional.ofNullable(dataStream)
                        .orElseThrow(() ->
                                new FlinkException(
                                        ErrCode.STREAMING_EXECUTION_FAILED,
                                        "Initialize Flink data stream failed, data stream must not be null"));
        this.context = context;
        this.typeInformation = dataStream.getType();
    }
}
