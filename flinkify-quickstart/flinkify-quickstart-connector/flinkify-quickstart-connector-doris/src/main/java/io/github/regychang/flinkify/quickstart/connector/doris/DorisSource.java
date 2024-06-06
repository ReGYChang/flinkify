package io.github.regychang.flinkify.quickstart.connector.doris;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;

import org.apache.flink.table.data.RowData;

public class DorisSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new DorisSource()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(RowData.class)).print();
    }
}
