package com.regy.quantalink.quickstart.connector.nebula.process;


import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.common.utils.HashUtils;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.nebula.operator.GraphProcessFunc;
import com.regy.quantalink.flink.core.connector.nebula.sink.NebulaSinkConnector;
import com.regy.quantalink.quickstart.connector.nebula.entity.NebulaTag;
import com.regy.quantalink.quickstart.connector.nebula.entity.SensorMap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author regy
 */
public class GenerateSensorMapGraph extends GraphProcessFunc<SensorMap> {

    public GenerateSensorMapGraph(Map<TypeInformation<?>, SinkConnector<?>> connectorMap) {
        super(connectorMap);
    }

    @Override
    protected void processGraph(SensorMap sensorMap, ProcessFunction<SensorMap, Void>.Context ctx, Collector<Void> collector, Map<TypeInformation<?>, SinkConnector<?>> connectorMap) throws Exception {
        for (SensorMap.ProductionLine line : sensorMap.productionLines) {
            extractProductionLineInfo(line, ctx, connectorMap);
        }
    }

    private void extractProductionLineInfo(SensorMap.ProductionLine line, ProcessFunction<SensorMap, Void>.Context ctx, Map<TypeInformation<?>, SinkConnector<?>> connectorMap) throws NoSuchAlgorithmException {
        NebulaSinkConnector lineSinkConnector =
                (NebulaSinkConnector) connectorMap.get(TypeInformation.get(NebulaTag.ProductionLine.class));

        Row lineRow = createLineRow(line, lineSinkConnector.getRowArity());
        ctx.output(lineSinkConnector.getOutputTag(), lineRow);

        Map<String, Tuple2<Integer, SensorMap.SequenceInfo>> sequenceMap = extractSequenceInfo(line.sequence);
        for (SensorMap.Workstation station : line.workstations) {
            extractStationInfo(lineRow.getFieldAs(0), station, sequenceMap, ctx, connectorMap);
        }
    }

    private Map<String, Tuple2<Integer, SensorMap.SequenceInfo>> extractSequenceInfo(List<SensorMap.SequenceInfo> sequence) {
        HashMap<String, Tuple2<Integer, SensorMap.SequenceInfo>> sequenceMap = new HashMap<>(256);
        for (int i = 0; i < sequence.size(); i++) {
            SensorMap.SequenceInfo sequenceInfo = sequence.get(i);
            sequenceMap.put(sequenceInfo.workstationOid, new Tuple2<>(i, sequenceInfo));
        }

        return sequenceMap;
    }

    private void extractStationInfo(
            String lineVid,
            SensorMap.Workstation station,
            Map<String, Tuple2<Integer, SensorMap.SequenceInfo>> sequenceMap,
            ProcessFunction<SensorMap, Void>.Context ctx,
            Map<TypeInformation<?>, SinkConnector<?>> connectorMap) throws NoSuchAlgorithmException {

        Tuple2<Integer, SensorMap.SequenceInfo> sequenceTuple = sequenceMap.get(station.oid);
        NebulaSinkConnector stationSinkConnector =
                (NebulaSinkConnector) connectorMap.get(TypeInformation.get(NebulaTag.Workstation.class));

        ctx.output(
                stationSinkConnector.getOutputTag(),
                createStationRow(
                        station,
                        stationSinkConnector.getRowArity(),
                        sequenceTuple));

        NebulaSinkConnector belongsToSinkConnector =
                (NebulaSinkConnector) connectorMap.get(TypeInformation.get(NebulaTag.BelongsTo.class));

        ctx.output(
                belongsToSinkConnector.getOutputTag(),
                createBelongsToRow(
                        lineVid,
                        HashUtils.hash(station.stationId),
                        belongsToSinkConnector.getRowArity()));
    }

    private Row createLineRow(SensorMap.ProductionLine line, Integer rowArity) throws NoSuchAlgorithmException {
        Row row = new Row(rowArity);

        row.setField(0, HashUtils.hash(line.lineId));
        row.setField(1, line.lineId);
        row.setField(2, line.oid);
        row.setField(3, line.name);

        return row;
    }

    private Row createStationRow(SensorMap.Workstation station, Integer rowArity, Tuple2<Integer, SensorMap.SequenceInfo> sequenceTuple) throws NoSuchAlgorithmException {
        Row row = new Row(rowArity);

        Integer sequencePosition = sequenceTuple.f0;
        SensorMap.SequenceInfo sequenceInfo = sequenceTuple.f1;

        row.setField(0, HashUtils.hash(station.stationId));
        row.setField(1, station.stationId);
        row.setField(2, station.oid);
        row.setField(3, station.name);
        row.setField(4, station.stationType);
        row.setField(5, String.valueOf(sequencePosition));
        row.setField(6, String.valueOf(sequenceInfo.baseOid));
        row.setField(7, String.valueOf(sequenceInfo.offset));
        row.setField(8, String.valueOf(sequenceInfo.processingTime));

        return row;
    }

    private Row createBelongsToRow(String lineVid, String stationVid, Integer rowArity) {
        Row row = new Row(rowArity);

        row.setField(0, stationVid);
        row.setField(1, lineVid);

        return row;
    }
}
