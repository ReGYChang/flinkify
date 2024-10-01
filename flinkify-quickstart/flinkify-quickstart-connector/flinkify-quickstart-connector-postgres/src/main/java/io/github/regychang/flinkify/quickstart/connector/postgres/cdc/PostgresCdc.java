package io.github.regychang.flinkify.quickstart.connector.postgres.cdc;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.kafka.serialization.CdcSourceRecordSerializationSchema;
import io.github.regychang.flinkify.flink.core.connector.kafka.serialization.KafkaSerializationAdapter;
import io.github.regychang.flinkify.flink.core.connector.kafka.sink.CachingTopicSelector;
import io.github.regychang.flinkify.flink.core.connector.postgres.serialization.JsonDebeziumDeserializationSchema;
import io.github.regychang.flinkify.flink.core.connector.serialization.SerializationAdapter;
import io.github.regychang.flinkify.flink.core.streaming.FlinkDataStream;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreaming;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingContext;
import io.github.regychang.flinkify.flink.core.streaming.FlinkStreamingInitializer;
import io.github.regychang.flinkify.flink.core.utils.debezium.DebeziumDeserializationAdapter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class PostgresCdc extends FlinkStreaming {

    private static final TypeInformation<SourceRecord> SOURCE_RECORD_TYPE_INFORMATION =
            TypeInformation.get(SourceRecord.class);

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer =
                new FlinkStreamingInitializer.Builder()
                        .withExecutionEnvironmentSetup(
                                env -> {
                                    env.enableCheckpointing(3000);

                                    // Flink job fails with `UnsupportedOperationException` on
                                    // unmodifiable collections.
                                    // Register `UnmodifiableCollectionsSerializer` from
                                    // `kryo-serializers` to fix.
                                    ExecutionConfig envConfig = env.getConfig();
                                    envConfig.addDefaultKryoSerializer(
                                            Collections.unmodifiableMap(new HashMap<>()).getClass(),
                                            UnmodifiableCollectionsSerializer.class);
                                    envConfig.addDefaultKryoSerializer(
                                            Collections.unmodifiableList(new ArrayList<>())
                                                    .getClass(),
                                            UnmodifiableCollectionsSerializer.class);
                                    envConfig.addDefaultKryoSerializer(
                                            Collections.unmodifiableSet(new HashSet<>()).getClass(),
                                            UnmodifiableCollectionsSerializer.class);
                                })
                        .withSourceConnectorSetup(
                                (sourceConnector, config) ->
                                        sourceConnector.withDeserializationSchemaAdapter(
                                                new DebeziumDeserializationAdapter<>(
                                                        new JsonDebeziumDeserializationSchema())),
                                TypeInformation.get(SourceRecord.class))
                        .withSinkConnectorSetup(
                                ((sinkConnector, config) ->
                                        sinkConnector.setSerializationAdapter(
                                                createSerializationAdapter())),
                                SOURCE_RECORD_TYPE_INFORMATION)
                        .build();

        (new PostgresCdc()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<SourceRecord> sink =
                ctx.getSourceDataStream(SOURCE_RECORD_TYPE_INFORMATION);
        sink.print();
//        FlinkDataStream.ofDataStream(sink, ctx).sink();
    }

    private static SerializationAdapter<SourceRecord, KafkaRecordSerializationSchema<SourceRecord>>
            createSerializationAdapter() {
        return new KafkaSerializationAdapter<>(
                createSerializationSchema(), SOURCE_RECORD_TYPE_INFORMATION);
    }

    private static CdcSourceRecordSerializationSchema createSerializationSchema() {
        return new CdcSourceRecordSerializationSchema(createTopicSelector());
    }

    private static CachingTopicSelector<SourceRecord> createTopicSelector() {
        return new CachingTopicSelector<>(ConnectRecord::topic);
    }
}
