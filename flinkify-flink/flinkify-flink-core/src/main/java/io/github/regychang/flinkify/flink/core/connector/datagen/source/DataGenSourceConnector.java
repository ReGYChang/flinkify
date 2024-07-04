package io.github.regychang.flinkify.flink.core.connector.datagen.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.datagen.serialization.DataGenDeserializationAdapter;

import io.github.regychang.java.faker.Faker;
import io.github.regychang.java.faker.Options;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

import static io.github.regychang.flinkify.flink.core.connector.datagen.config.DataGenOptions.NUMBER_OF_ROWS;
import static io.github.regychang.flinkify.flink.core.connector.datagen.config.DataGenOptions.ROWS_PER_SECOND;

public class DataGenSourceConnector<T> extends SourceConnector<T> {

    private final long numberOfRows;

    private final long rowsPerSecond;

    public DataGenSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.numberOfRows = config.get(NUMBER_OF_ROWS);
        this.rowsPerSecond = config.get(ROWS_PER_SECOND);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        @SuppressWarnings("unchecked")
        DataGenDeserializationAdapter<T> deserializationAdapter =
                Optional.ofNullable((DataGenDeserializationAdapter<T>) getDeserializationAdapter())
                        .orElse(new DataGenDeserializationAdapter<>(getConfig()));

        Faker faker = new Faker();
        Options options = deserializationAdapter.getDeserializationSchema();
        GeneratorFunction<Long, T> generatorFunction =
                createGeneratorFunction(faker, getTypeInfo().getRawType(), options);

        DataGeneratorSource<T> source =
                new DataGeneratorSource<>(
                        generatorFunction,
                        numberOfRows,
                        RateLimiterStrategy.perSecond(rowsPerSecond),
                        getTypeInfo().toFlinkType());

        return getEnv().fromSource(source, WatermarkStrategy.noWatermarks(), getName());
    }

    private GeneratorFunction<Long, T> createGeneratorFunction(Faker faker, Class<T> clazz, Options options) {
        return index -> {
            try {
                return faker.fakeData(clazz, options);
            } catch (Exception e) {
                throw new FlinkException(
                        ErrCode.STREAMING_CONNECTOR_FAILED,
                        String.format(
                                "Failed to create fake data generator due to: %s",
                                e.getMessage()));
            }
        };
    }
}
