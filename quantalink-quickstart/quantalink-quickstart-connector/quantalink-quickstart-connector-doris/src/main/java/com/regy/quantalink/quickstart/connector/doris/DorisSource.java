package com.regy.quantalink.quickstart.connector.doris;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;

import org.apache.flink.table.data.RowData;

/**
 * @author regy
 */
public class DorisSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new DorisSource()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(RowData.class)).print();
    }
}
