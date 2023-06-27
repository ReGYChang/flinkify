package com.regy.quantalink.quickstart.connector.kafka;


import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.connector.kafka.entity.DcsEvent;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

/**
 * @author regy
 */
public class KafkaSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()
                .withSourceConnectorSetup(
                        (sourceConnector, config) ->
                                sourceConnector.withDeserializationSchemaAdapter(KafkaDeserializationAdapter.valueOnlyDefault(TypeInformation.get(DcsEvent.class)))
                                        .withWatermarkStrategy(WatermarkStrategy.noWatermarks()),
                        TypeInformation.get(DcsEvent.class)).build();

        (new KafkaSource()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        DataStreamSource<DcsEvent> sourceStream = context.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        sourceStream.print();
    }
}
