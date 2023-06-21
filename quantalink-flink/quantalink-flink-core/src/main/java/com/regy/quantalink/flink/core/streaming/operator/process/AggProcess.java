package com.regy.quantalink.flink.core.streaming.operator.process;

import java.io.Serializable;

/**
 * @author regy
 */
@FunctionalInterface
public interface AggProcess<IN, ACC> extends Serializable {
    ACC aggregate(IN var1, ACC var2);
}
