package com.regy.quantalink.quickstart.datastream;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.FlinkOptions;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkDataStream;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.datastream.entity.DcsEvent;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


/**
 * @author regy
 */
public class SimpleAppWithConfig extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new FlinkStreamingInitializer.Builder()

                        .withConfigurationSetup(
                                (config) -> {
                                    config.set(FlinkOptions.JOB_NAME, "Awesome Job");
                                })

                        .withExecutionEnvironmentSetup(
                                (env) -> {
                                    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
                                })

                        .withSourceConnectorSetup(
                                (sourceConnector) -> {
                                    sourceConnector.withDeserializationSchemaAdapter(KafkaDeserializationAdapter.valueOnlyDefault(TypeInformation.get(DcsEvent.class)));
                                },
                                TypeInformation.get(DcsEvent.class)).build();

        (new SimpleAppWithConfig()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        DataStreamSource<DcsEvent> sourceDataStream = context.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        FlinkDataStream.ofDataStream(sourceDataStream, context).sink();
    }
}
