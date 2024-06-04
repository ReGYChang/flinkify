package com.regy.quantalink.quickstart.connector.datagen.source;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.datagen.serialization.DataGenDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;

import com.alibaba.fastjson.JSON;

public class DataGenSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new FlinkStreamingInitializer.Builder()
                        .withSourceConnectorSetup(
                                ((sourceConnector, config) ->
                                        sourceConnector.withDeserializationSchemaAdapter(
                                                new DataGenDeserializationAdapter<>(config))),
                                TypeInformation.get(SimplePojo.class))
                        .build();

        (new DataGenSource()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        ctx.getSourceDataStream(TypeInformation.get(SimplePojo.class))
                .map(JSON::toJSONString)
                .print();
    }
}
