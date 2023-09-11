package com.regy.quantalink.flink.core.connector.doris.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;

public class ObjectStringTransformFunc<T> implements FlatMapFunction<T, String> {

    @Override
    public void flatMap(T object, Collector<String> collector) throws Exception {
        StringBuilder sb = new StringBuilder();
        Field[] fields = object.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            sb.append(field.get(object)).append("\t");
        }

        String result = sb.toString().trim();
        collector.collect(result);
    }
}
