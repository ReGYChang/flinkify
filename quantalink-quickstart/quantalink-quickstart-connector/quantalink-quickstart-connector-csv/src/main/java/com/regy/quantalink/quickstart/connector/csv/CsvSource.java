package com.regy.quantalink.quickstart.connector.csv;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.quickstart.connector.csv.entity.SaleCase;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @author regy
 */
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
