package com.regy.quantalink.flink.core.streaming.operator;

/**
 * @author regy
 */
public interface UserDefinedTimeKey<T> {

    T getValue();

    String getKey();

    default Integer getKeyHash() {
        return this.getKey().hashCode();
    }

    Long getTimestamp();

    Long getUpperBound();

    Long getLowerBound();
}
