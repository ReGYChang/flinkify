package io.github.regychang.flinkify.quickstart.connector.postgres.sink;

import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.streaming.FlinkDataStream;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.java.faker.Faker;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresSink extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        (new PostgresSink()).run(args);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        StreamExecutionEnvironment env = ctx.getEnv();
        Faker faker = new Faker();
        DataStreamSource<Inventory> source =
                env.fromElements(
                        faker.fakeData(Inventory.class),
                        faker.fakeData(Inventory.class),
                        faker.fakeData(Inventory.class));

        FlinkDataStream.ofDataStream(source, ctx).sink();
    }
}
