package io.github.regychang.flinkify.quickstart.connector.sqlserver.source;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer.Builder;
import io.github.regychang.flinkify.flink.core.utils.debezium.DebeziumDeserializationAdapter;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

public class SqlServerCdc extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new Builder()
                        .withExecutionEnvironmentSetup(
                                env -> {
                                    env.enableCheckpointing(10000);
                                    env.disableOperatorChaining();
                                })
                        .withSourceConnectorSetup(
                                (connector, config) ->
                                        connector.withDeserializationSchemaAdapter(
                                                new DebeziumDeserializationAdapter<>(
                                                        new JsonDebeziumDeserializationSchema(
                                                                true))),
                                TypeInformation.get(String.class))
                        .build();

        (new SqlServerCdc()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext context) throws FlinkException {
        context.getSourceDataStream(TypeInformation.get(String.class)).print();
    }
}
