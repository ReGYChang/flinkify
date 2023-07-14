package com.regy.quantalink.quickstart.connector.oracle.cdc;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;

/**
 * @author regy
 */
public class OracleCdc extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new OracleCdc()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(String.class)).print();
    }
}
