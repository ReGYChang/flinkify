package io.github.regychang.flinkify.quickstart.connector.postgres.cdc;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;

public class PostgresCdc extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new FlinkStreamingInitializer.Builder()
                        .withExecutionEnvironmentSetup(env -> env.enableCheckpointing(3000))
                        .build();

        (new PostgresCdc()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(String.class)).print();
    }
}
