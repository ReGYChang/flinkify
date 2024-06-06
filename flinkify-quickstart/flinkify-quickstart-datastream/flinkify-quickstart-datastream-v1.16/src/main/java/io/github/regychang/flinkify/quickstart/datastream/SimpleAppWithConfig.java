package io.github.regychang.flinkify.quickstart.datastream;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.config.FlinkOptions;
import io.github.regychang.flinkify.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;
import io.github.regychang.flinkify.flink.core.streaming.FlinkDataStream;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;
import io.github.regychang.flinkify.quickstart.datastream.entity.DcsEvent;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


public class SimpleAppWithConfig extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new FlinkStreamingInitializer.Builder()
                        .withConfigurationSetup(
                                (config) ->
                                        config.set(FlinkOptions.JOB_NAME, "Awesome Job"))

                        .withExecutionEnvironmentSetup(
                                (env) ->
                                        env.setRuntimeMode(RuntimeExecutionMode.STREAMING))

                        .withSourceConnectorSetup(
                                (sourceConnector, config) ->
                                        sourceConnector.withDeserializationSchemaAdapter(
                                                KafkaDeserializationAdapter.valueOnlyDefault(
                                                        TypeInformation.get(DcsEvent.class))),
                                TypeInformation.get(DcsEvent.class))

                        .build();

        (new SimpleAppWithConfig()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        DataStreamSource<DcsEvent> sourceDataStream =
                context.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        FlinkDataStream.ofDataStream(sourceDataStream, context).sink();
    }
}
