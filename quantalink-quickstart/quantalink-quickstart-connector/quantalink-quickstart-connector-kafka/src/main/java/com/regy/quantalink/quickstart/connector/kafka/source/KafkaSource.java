package com.regy.quantalink.quickstart.connector.kafka.source;


import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @author regy
 */
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
