package com.regy.quantalink.flink.core.streaming.operator;

import com.regy.quantalink.flink.core.streaming.operator.process.AggProcess;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @author regy
 */
public class KeyedAggProcessFunc<K, IN extends UserDefinedTimeKey<IN>, OUT> extends KeyedProcessFunction<K, IN, OUT> {
    private transient MapState<Long, Map<String, OUT>> buffer;
    private final TypeInformation<OUT> outputTypeInformation;
    private final OutputTag<IN> outputTag;
    private final AggProcess<IN, OUT> userFunction;
    private static final String BUFFER = "BUFFER";

    public KeyedAggProcessFunc(TypeInformation<OUT> outputTypeInformation, OutputTag<IN> outputTag, AggProcess<IN, OUT> userFunction) {
        this.outputTypeInformation = Preconditions.checkNotNull(outputTypeInformation);
        this.outputTag = Preconditions.checkNotNull(outputTag);
        this.userFunction = Preconditions.checkNotNull(userFunction);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        buffer = this.getRuntimeContext().getMapState(new MapStateDescriptor<>(BUFFER, Types.LONG, Types.MAP(Types.STRING, outputTypeInformation)));
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<K, IN, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception {
        buffer.get(timestamp).values().forEach(out::collect);
        buffer.remove(timestamp);
    }

    @Override
    public void processElement(IN event, KeyedProcessFunction<K, IN, OUT>.Context ctx, Collector<OUT> collector) throws Exception {
        String key = event.getKey();

        long cleanupTimestamp = event.getUpperBound();
        long currentWatermark = ctx.timerService().currentWatermark();

        if (currentWatermark < cleanupTimestamp) {
            addToBuffer(event, cleanupTimestamp, key);
            ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
        } else {
            ctx.output(outputTag, event);
        }
    }

    private void addToBuffer(IN event, long timestamp, String key) throws Exception {
        Map<String, OUT> elemsInMap = buffer.get(timestamp);
        if (elemsInMap == null) {
            elemsInMap = new HashMap<>(256);
        }

        OUT record = userFunction.aggregate(event, elemsInMap.get(key));
        elemsInMap.put(key, record);

        buffer.put(timestamp, elemsInMap);
    }
}