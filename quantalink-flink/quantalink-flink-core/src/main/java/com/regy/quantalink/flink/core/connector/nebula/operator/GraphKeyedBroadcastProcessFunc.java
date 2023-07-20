package com.regy.quantalink.flink.core.connector.nebula.operator;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.common.utils.CopyUtils;
import com.regy.quantalink.flink.core.connector.SinkConnector;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author regy
 */
public abstract class GraphKeyedBroadcastProcessFunc<K, IN1, IN2> extends KeyedBroadcastProcessFunction<K, IN1, IN2, Void> {
    protected final Map<TypeInformation<?>, SinkConnector<?, ?>> connectorMap;

    public GraphKeyedBroadcastProcessFunc(Map<TypeInformation<?>, SinkConnector<?, ?>> connectorMap) {
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
            Map<TypeInformation<?>, SinkConnector<?, ?>> connectorMap) throws Exception;

}
