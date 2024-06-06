package io.github.regychang.flinkify.flink.core.streaming.operator;

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
