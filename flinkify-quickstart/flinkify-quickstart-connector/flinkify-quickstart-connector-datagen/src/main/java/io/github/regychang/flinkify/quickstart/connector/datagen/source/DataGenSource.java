package io.github.regychang.flinkify.quickstart.connector.datagen.source;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.datagen.serialization.DataGenDeserializationAdapter;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;

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
