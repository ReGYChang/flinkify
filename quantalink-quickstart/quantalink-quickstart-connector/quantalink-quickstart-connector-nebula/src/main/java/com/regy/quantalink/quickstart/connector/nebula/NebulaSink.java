package com.regy.quantalink.quickstart.connector.nebula;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.kafka.serialization.KafkaDeserializationAdapter;
import com.regy.quantalink.flink.core.streaming.FlinkDataStream;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.connector.nebula.entity.DcsEvent;
import com.regy.quantalink.quickstart.connector.nebula.entity.NebulaTag;
import com.regy.quantalink.quickstart.connector.nebula.entity.SensorMap;
import com.regy.quantalink.quickstart.connector.nebula.process.GenerateSensorMapGraph;
import com.regy.quantalink.quickstart.connector.nebula.process.GenerateWorkOrderGraph;
import com.regy.quantalink.quickstart.connector.nebula.serialization.PayloadDeserializationSchema;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.List;
import java.util.Map;

/**
 * @author regy
 */
public class NebulaSink extends FlinkStreaming {

    private final MapStateDescriptor<String, Map<String, List<String>>> SENSOR_MAP_DESC =
            new MapStateDescriptor<>("sensor_map-state", Types.STRING, Types.MAP(Types.STRING, Types.LIST(Types.STRING)));

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

        (new NebulaSink()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {

        // create sensor map graph
        DataStreamSource<SensorMap> mapSourceStream = ctx.getSourceDataStream(TypeInformation.get(SensorMap.class));
        SingleOutputStreamOperator<Void> mapGraphStream = mapSourceStream.process(new GenerateSensorMapGraph(ctx.getSinkConnectors()));

        // TODO: refactor does not test
        FlinkDataStream.ofDataStream(mapGraphStream, ctx)
                .sink(TypeInformation.get(NebulaTag.ProductionLine.class))
                .sink(TypeInformation.get(NebulaTag.Workstation.class))
                .sink(TypeInformation.get(NebulaTag.BelongsTo.class));


        // create product graph
        SingleOutputStreamOperator<Void> dcsGraphStream = ctx.getSourceDataStream(TypeInformation.get(DcsEvent.class))
                .keyBy(record -> record.dcsId)
                .connect(mapSourceStream.broadcast(SENSOR_MAP_DESC))
                .process(new GenerateWorkOrderGraph(ctx.getSinkConnectors(), SENSOR_MAP_DESC));

        FlinkDataStream.ofDataStream(dcsGraphStream, ctx)
                .sink(TypeInformation.get(NebulaTag.WorkOrder.class))
                .sink(TypeInformation.get(NebulaTag.ProducedOn.class));
    }
}

/*
CREATE SPACE esg (partition_num = 10, vid_type = FIXED_STRING(64));
CREATE tag `production_line` (`oid` string NOT NULL, `id` string NOT NULL, `name` string NOT NULL  );
CREATE tag `work_station` (`oid` string NOT NULL, `id` string NOT NULL, `name` string NOT NULL  , `station_type` string NOT NULL, `sequence_position` int NOT NULL, `base_station_id` string NOT NULL, `offset` double NOT NULL, `processing_time` double NOT NULL, );
CREATE tag `work_order` (`work_order_id` string NOT NULL  );
CREATE edge `belongs_to` ();
CREATE edge `produced_on` (`started_at` datetime NOT NULL, `ended_at` datetime NOT NULL, `produced_count` int64 NOT NULL);
 */
