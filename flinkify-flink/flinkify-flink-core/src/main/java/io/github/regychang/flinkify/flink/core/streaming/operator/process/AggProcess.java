package io.github.regychang.flinkify.flink.core.streaming.operator.process;

import java.io.Serializable;

@FunctionalInterface
public interface AggProcess<IN, ACC> extends Serializable {
    ACC aggregate(IN var1, ACC var2);
}
