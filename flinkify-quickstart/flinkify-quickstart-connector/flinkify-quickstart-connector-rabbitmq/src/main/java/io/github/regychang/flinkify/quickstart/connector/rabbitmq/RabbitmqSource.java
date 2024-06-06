package io.github.regychang.flinkify.quickstart.connector.rabbitmq;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.common.utils.TimeUtils;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.serialization.RabbitmqDeserializationAdapter;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;
import io.github.regychang.flinkify.quickstart.connector.rabbitmq.entity.DcsEvent;
import io.github.regychang.flinkify.quickstart.connector.rabbitmq.serialization.PayloadDeserializer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.time.Duration;

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
                                        new RabbitmqDeserializationAdapter<>(
                                                new PayloadDeserializer<>(
                                                        TypeInformation.get(
                                                                DcsEvent.class,
                                                                DcsEvent.FromRabbitmq.class)))),
                        TypeInformation.get(
                                DcsEvent.class,
                                DcsEvent.FromRabbitmq.class))
                .build();

        (new RabbitmqSource()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<DcsEvent> sourceStream = ctx.getSourceDataStream(TypeInformation.get(DcsEvent.class));
        sourceStream.print();
    }
}
