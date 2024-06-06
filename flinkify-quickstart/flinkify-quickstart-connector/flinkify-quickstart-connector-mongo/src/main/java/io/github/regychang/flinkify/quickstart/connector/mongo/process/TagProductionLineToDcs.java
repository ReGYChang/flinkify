package io.github.regychang.flinkify.quickstart.connector.mongo.process;

import io.github.regychang.flinkify.quickstart.connector.mongo.entity.DcsEvent;
import io.github.regychang.flinkify.quickstart.connector.mongo.entity.Record;
import io.github.regychang.flinkify.quickstart.connector.mongo.entity.SensorMap;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class TagProductionLineToDcs extends BroadcastProcessFunction<DcsEvent, SensorMap, Record> {

    private final MapStateDescriptor<String, Map<String, SensorMap.ProductionLine>> stateDesc;

    private volatile boolean isReady = false;

    public TagProductionLineToDcs(MapStateDescriptor<String, Map<String, SensorMap.ProductionLine>> stateDesc) {
        this.stateDesc = stateDesc;
    }

    @Override
    public void processElement(
            DcsEvent dcsEvent,
            BroadcastProcessFunction<DcsEvent, SensorMap, Record>.ReadOnlyContext readOnlyContext,
            Collector<Record> collector) throws Exception {
        if (isReady) {
            ReadOnlyBroadcastState<String, Map<String, SensorMap.ProductionLine>> state =
                    readOnlyContext.getBroadcastState(stateDesc);
            for (Map.Entry<String, Map<String, SensorMap.ProductionLine>> entry : state.immutableEntries()) {
                SensorMap.ProductionLine productionLine = entry.getValue().get(dcsEvent.getDcsId());
                collector.collect(
                        new Record(
                                dcsEvent.getSerialNumber(),
                                dcsEvent.getStartedAt(),
                                dcsEvent.getEndedAt(),
                                productionLine.lineId,
                                productionLine.name));
            }
        }
    }

    @Override
    public void processBroadcastElement(
            SensorMap sensorMap,
            BroadcastProcessFunction<DcsEvent, SensorMap, Record>.Context context,
            Collector<Record> collector) throws Exception {
        BroadcastState<String, Map<String, SensorMap.ProductionLine>> state = context.getBroadcastState(stateDesc);
        state.put(sensorMap.oid, extractProductionLine(sensorMap));
        isReady = true;
    }

    private Map<String, SensorMap.ProductionLine> extractProductionLine(SensorMap sensorMap) {
        Map<String, SensorMap.ProductionLine> map = new HashMap<>(256);
        for (SensorMap.ProductionLine line : sensorMap.productionLines) {
            for (SensorMap.Workstation station : line.workstations) {
                map.put(station.stationId, line);
            }
        }
        return map;
    }
}
