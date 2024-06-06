package io.github.regychang.flinkify.flink.core.streaming.operator;

import io.github.regychang.flinkify.flink.core.streaming.operator.process.JoinProcess;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IntervalJoinByUnionKey<K, T1 extends UserDefinedTimeKey<T1>, T2 extends UserDefinedTimeKey<T2>, OUT>
        extends KeyedCoProcessFunction<K, T1, T2, OUT> {

    private transient MapState<Long, Map<String, List<T1>>> leftBuffer;

    private transient MapState<Long, Map<String, List<T2>>> rightBuffer;

    private final TypeInformation<T1> leftTypeInformation;

    private final TypeInformation<T2> rightTypeInformation;

    private final Class<T1> leftClass;

    private final Class<T2> rightClass;

    private static final String LEFT_BUFFER = "LEFT_BUFFER";

    private static final String RIGHT_BUFFER = "RIGHT_BUFFER";

    private final OutputTag<Tuple2<T1, T2>> outputTag;

    private final JoinProcess<K, T1, T2, OUT> joinFunction;

    private final Long rightTtl;

    public IntervalJoinByUnionKey(
            JoinProcess<K, T1, T2, OUT> joinFunction,
            Long rightTtl,
            OutputTag<Tuple2<T1, T2>> outputTag,
            Class<T1> leftClass,
            Class<T2> rightClass) {
        this.joinFunction = Preconditions.checkNotNull(joinFunction);
        this.outputTag = Preconditions.checkNotNull(outputTag);
        this.leftClass = Preconditions.checkNotNull(leftClass);
        this.rightClass = Preconditions.checkNotNull(rightClass);
        this.leftTypeInformation = Types.POJO(leftClass);
        this.rightTypeInformation = Types.POJO(rightClass);
        this.rightTtl = Preconditions.checkNotNull(rightTtl);
    }

    @Override
    public void open(Configuration parameters) {
        leftBuffer =
                this.getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        LEFT_BUFFER,
                                        Types.LONG,
                                        Types.MAP(Types.STRING, Types.LIST(leftTypeInformation))));
        rightBuffer =
                this.getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        RIGHT_BUFFER,
                                        Types.LONG,
                                        Types.MAP(Types.STRING, Types.LIST(rightTypeInformation))));
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedCoProcessFunction<K, T1, T2, OUT>.OnTimerContext ctx,
            Collector<OUT> collector) throws Exception {
        joinFunction.onTimer(ctx, collector, outputTag, leftBuffer.get(timestamp), rightBuffer.get(timestamp));
        leftBuffer.remove(timestamp);
        rightBuffer.remove(timestamp);
    }

    @Override
    public void processElement1(
            T1 record,
            KeyedCoProcessFunction<K, T1, T2, OUT>.Context ctx,
            Collector<OUT> collector) throws Exception {
        this.processElement(record, leftBuffer, rightBuffer, true, ctx, collector);
    }

    @Override
    public void processElement2(
            T2 record,
            KeyedCoProcessFunction<K, T1, T2, OUT>.Context ctx,
            Collector<OUT> collector) throws Exception {
        this.processElement(record, rightBuffer, leftBuffer, false, ctx, collector);
    }

    private <THIS, OTHER extends UserDefinedTimeKey<OTHER>> void processElement(
            UserDefinedTimeKey<THIS> record,
            MapState<Long, Map<String, List<THIS>>> ourBuffer,
            MapState<Long, Map<String, List<OTHER>>> otherBuffer,
            boolean isLeft,
            KeyedCoProcessFunction<K, T1, T2, OUT>.Context ctx,
            Collector<OUT> collector) throws Exception {
        String key = record.getKey();
        THIS value = record.getValue();

        long ourTimestamp = record.getTimestamp();
        long cleanupTimestamp = record.getUpperBound();
        long currentWatermark = ctx.timerService().currentWatermark();

        if (currentWatermark < cleanupTimestamp) {
            addToBuffer(ourBuffer, value, cleanupTimestamp, key);
            Iterator<Map.Entry<Long, Map<String, List<OTHER>>>> otherMapIt = otherBuffer.entries().iterator();

            while (true) {
                Map.Entry<Long, Map<String, List<OTHER>>> otherBucket;
                long leftUpperBound, leftLowerBound, rightTimestamp;

                do {
                    if (!otherMapIt.hasNext()) {
                        ctx.timerService().registerEventTimeTimer(cleanupTimestamp);
                        return;
                    }

                    otherBucket = otherMapIt.next();
                    leftUpperBound = isLeft ? record.getUpperBound() : otherBucket.getKey();
                    rightTimestamp = isLeft ? otherBucket.getKey() - rightTtl : ourTimestamp;
                } while (leftUpperBound < rightTimestamp);

                List<OTHER> otherList = otherBucket.getValue().get(key);

                if (otherList != null) {
                    for (OTHER otherValue : otherList) {
                        leftLowerBound =
                                isLeft ?
                                        record.getLowerBound() : otherValue.getLowerBound();
                        if (leftLowerBound < rightTimestamp) {
                            if (isLeft) {
                                collector.collect(
                                        joinFunction.join(
                                                leftClass.cast(value),
                                                rightClass.cast(otherValue)));
                            } else {
                                collector.collect(
                                        joinFunction.join(
                                                leftClass.cast(otherValue),
                                                rightClass.cast(value)));
                            }
                        }
                    }
                }
            }

        } else {
            ctx.output(outputTag, isLeft ?
                    new Tuple2<>(leftClass.cast(value), null) :
                    new Tuple2<>(null, rightClass.cast(value)));
        }
    }

    private static <T> void addToBuffer(
            MapState<Long, Map<String, List<T>>> buffer, T value, long timestamp, String key) throws Exception {
        Map<String, List<T>> elemsInMap = buffer.get(timestamp);
        if (elemsInMap == null) {
            elemsInMap = new HashMap<>(256);
        }

        List<T> elemsInBucket = elemsInMap.get(key);
        if (elemsInBucket == null) {
            elemsInBucket = new ArrayList<>();
        }

        elemsInBucket.add(value);
        elemsInMap.put(key, elemsInBucket);

        buffer.put(timestamp, elemsInMap);
    }

    @Override
    public void close() {
        leftBuffer.clear();
        rightBuffer.clear();
    }
}
