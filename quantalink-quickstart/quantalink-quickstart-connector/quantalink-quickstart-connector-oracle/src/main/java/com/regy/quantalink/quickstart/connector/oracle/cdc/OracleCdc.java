package com.regy.quantalink.quickstart.connector.oracle.cdc;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;

/**
 * @author regy
 */
public class OracleCdc extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()
                .withExecutionEnvironmentSetup(
                        env -> {
                            env.enableCheckpointing(3000);
                            env.setParallelism(1);
                        }).build();

        (new OracleCdc()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(String.class)).print();
    }
}
