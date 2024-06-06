package io.github.regychang.flinkify.quickstart.connector.kafka.source;


import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class KafkaSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new KafkaSource()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        DataStreamSource<String> sourceStream = context.getSourceDataStream(TypeInformation.get(String.class));
        sourceStream.print();
    }
}
