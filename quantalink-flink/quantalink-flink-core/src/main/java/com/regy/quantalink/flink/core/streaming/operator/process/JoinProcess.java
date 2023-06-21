package com.regy.quantalink.flink.core.streaming.operator.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author regy
 */
public interface JoinProcess<K, IN1, IN2, OUT> extends Serializable {
    OUT join(IN1 var1, IN2 var2);

    void onTimer(KeyedCoProcessFunction<K, IN1, IN2, OUT>.OnTimerContext ctx, Collector<OUT> collector,
                 OutputTag<Tuple2<IN1, IN2>> outputTag, Map<String, List<IN1>> map1, Map<String, List<IN2>> map2);
}
