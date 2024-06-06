package io.github.regychang.flinkify.flink.core.connector.nebula.operator;

import io.github.regychang.flinkify.common.utils.CopyUtils;
import io.github.regychang.flinkify.flink.core.connector.ConnectorKey;
import io.github.regychang.flinkify.flink.core.connector.SinkConnector;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public abstract class GraphKeyedBroadcastProcessFunc<K, IN1, IN2>
        extends KeyedBroadcastProcessFunction<K, IN1, IN2, Void> {

    protected final Map<ConnectorKey<?>, SinkConnector<?, ?>> connectorMap;

    public GraphKeyedBroadcastProcessFunc(Map<ConnectorKey<?>, SinkConnector<?, ?>> connectorMap) {
        this.connectorMap = CopyUtils.deepCopy(connectorMap);
    }

    @Override
    public void processElement(
            IN1 input,
            KeyedBroadcastProcessFunction<K, IN1, IN2, Void>.ReadOnlyContext readOnlyCtx,
            Collector<Void> collector) throws Exception {
        processGraph(input, readOnlyCtx, collector, connectorMap);
    }

    protected abstract void processGraph(
            IN1 input,
            KeyedBroadcastProcessFunction<K, IN1, IN2, Void>.ReadOnlyContext readOnlyCtx,
            Collector<Void> collector,
            Map<ConnectorKey<?>, SinkConnector<?, ?>> connectorMap) throws Exception;
}
