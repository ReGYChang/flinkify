package com.regy.quantalink.flink.core.connector.doris.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;
import java.util.Optional;

public class ObjectStringTransformFunc<T> implements FlatMapFunction<T, String> {

    @Override
    public void flatMap(T object, Collector<String> collector) throws Exception {
        StringBuilder sb = new StringBuilder();
        Field[] fields = object.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            String stringValue =
                    Optional.ofNullable(field.get(object))
                            .map(value -> value.toString().trim())
                            .orElse(null);

            sb.append(stringValue).append("\t");
        }

        String result = sb.toString().trim();
        collector.collect(result);
    }
}
