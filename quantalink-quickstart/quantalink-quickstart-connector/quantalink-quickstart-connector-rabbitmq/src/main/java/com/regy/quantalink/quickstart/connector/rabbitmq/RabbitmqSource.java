package com.regy.quantalink.quickstart.connector.rabbitmq;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.common.utils.TimeUtils;
import com.regy.quantalink.flink.core.connector.rabbitmq.serialization.RabbitmqDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.connector.rabbitmq.entity.DcsEvent;
import com.regy.quantalink.quickstart.connector.rabbitmq.serialization.PayloadDeserializer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.time.Duration;

/**
 * @author regy
 */
public class RabbitmqSource extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()

                .withExecutionEnvironmentSetup(
                        env -> {
                            env.enableCheckpointing(TimeUtils.toMillis(Duration.ofMinutes(10)));
                            env.setParallelism(1);
                        })

                .withSourceConnectorSetup(
                        (sourceConnector, config) ->
                                sourceConnector.withDeserializationSchemaAdapter(
                                        new RabbitmqDeserializationAdapter<>(new PayloadDeserializer<>(TypeInformation.get(DcsEvent.class, DcsEvent.FromRabbitmq.class)))),
                        TypeInformation.get(DcsEvent.class, DcsEvent.FromRabbitmq.class)).build();

        (new RabbitmqSource()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<DcsEvent> sourceStream = ctx.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        sourceStream.print();
    }
}
