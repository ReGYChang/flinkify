package io.github.regychang.flinkify.quickstart.connector.csv;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.quickstart.connector.csv.entity.SaleCase;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class CsvSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new CsvSource()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<SaleCase> sourceStream = ctx.getSourceDataStream(TypeInformation.get(SaleCase.class));
        sourceStream.print();
    }
}
