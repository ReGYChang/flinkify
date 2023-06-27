package com.regy.quantalink.quickstart.connector.mongo;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkDataStream;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.connector.mongo.entity.DcsEvent;
import com.regy.quantalink.quickstart.connector.mongo.entity.Record;
import com.regy.quantalink.quickstart.connector.mongo.entity.SensorMap;
import com.regy.quantalink.quickstart.connector.mongo.process.TagProductionLineToDcs;
import com.regy.quantalink.quickstart.connector.mongo.serialization.PayloadDeserializationSchema;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

/**
 * @author regy
 */
public class MongoSink extends FlinkStreaming {

    private final MapStateDescriptor<String, Map<String, SensorMap.ProductionLine>> SENSOR_MAP_DESC =
            new MapStateDescriptor<>("sensor_map-state", Types.STRING, Types.MAP(Types.STRING, Types.POJO(SensorMap.ProductionLine.class)));

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()
                .withExecutionEnvironmentSetup(
                        env -> {
                            env.enableCheckpointing(3000);
                            env.setParallelism(1);
                        })
                .withSourceConnectorSetup(
                        (sourceConnector, config) ->
                                sourceConnector.withDeserializationSchemaAdapter(
                                        KafkaDeserializationAdapter.valueOnly(new PayloadDeserializationSchema<>(TypeInformation.get(DcsEvent.class)))),
                        TypeInformation.get(DcsEvent.class)).build();

        (new MongoSink()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {

        DataStreamSource<SensorMap> mapSourceStream = ctx.getSourceDataStream(TypeInformation.get(SensorMap.class));
        SingleOutputStreamOperator<Record> stream = ctx.getSourceDataStream(TypeInformation.get(DcsEvent.class))
                .connect(mapSourceStream.broadcast(SENSOR_MAP_DESC))
                .process(new TagProductionLineToDcs(SENSOR_MAP_DESC));

        FlinkDataStream.ofDataStream(stream, ctx).sink();
    }
}
