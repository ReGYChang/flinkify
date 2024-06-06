package io.github.regychang.flinkify.quickstart.connector.kafka.sink;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.streaming.FlinkDataStream;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class KafkaSink extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new KafkaSink()).run(args);

    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<String> testSourceStream = ctx.getEnv().fromElements("test", "test1");
        FlinkDataStream.ofDataStream(testSourceStream, ctx).sink();
    }
}
