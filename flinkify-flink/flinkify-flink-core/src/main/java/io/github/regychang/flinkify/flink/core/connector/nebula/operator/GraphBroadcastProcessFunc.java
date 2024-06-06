package io.github.regychang.flinkify.flink.core.connector.nebula.operator;

import io.github.regychang.flinkify.flink.core.connector.SinkConnector;

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public abstract class GraphBroadcastProcessFunc<IN1, IN2> extends BroadcastProcessFunction<IN1, IN2, Void> {

    protected final Map<Class<?>, SinkConnector<?, ?>> connectorMap;

    public GraphBroadcastProcessFunc(Map<Class<?>, SinkConnector<?, ?>> connectorMap) {
        this.connectorMap = connectorMap;
    }

    @Override
    public void processElement(
            IN1 input,
            BroadcastProcessFunction<IN1, IN2, Void>.ReadOnlyContext readOnlyCtx,
            Collector<Void> collector) throws Exception {

        processGraph(input, readOnlyCtx, collector, connectorMap);
    }

    protected abstract void processGraph(
            IN1 input,
            BroadcastProcessFunction<IN1, IN2, Void>.ReadOnlyContext readOnlyFuncCtx,
            Collector<Void> collector,
            Map<Class<?>, SinkConnector<?, ?>> connectorMap) throws Exception;
}
