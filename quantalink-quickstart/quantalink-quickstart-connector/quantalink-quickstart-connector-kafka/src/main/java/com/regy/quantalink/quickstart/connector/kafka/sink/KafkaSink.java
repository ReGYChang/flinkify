package com.regy.quantalink.quickstart.connector.kafka.sink;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.streaming.FlinkDataStream;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;

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
