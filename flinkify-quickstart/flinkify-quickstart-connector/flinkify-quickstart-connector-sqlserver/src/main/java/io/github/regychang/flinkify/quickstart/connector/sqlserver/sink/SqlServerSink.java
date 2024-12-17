package io.github.regychang.flinkify.quickstart.connector.sqlserver.sink;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkDataStream;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SqlServerSink extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new SqlServerSink()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        StreamExecutionEnvironment env = context.getEnv();
        env.setParallelism(1);
        DataStreamSource<IPBDHU> sourceStream =
                context.getSourceDataStream(TypeInformation.get(IPBDHU.class));
        FlinkDataStream.ofDataStream(sourceStream, context).sink();
    }
}
