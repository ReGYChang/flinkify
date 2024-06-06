package io.github.regychang.flinkify.flink.core.connector.kafka.config;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public enum OffsetInitializationType {

    COMMITTED_WITH_RESET_STRATEGY,

    TIMESTAMP,

    EARLIEST,

    LATEST;

    private OffsetResetStrategy resetStrategy;

    private long timestamp;

    public OffsetInitializationType withResetStrategy(OffsetResetStrategy resetStrategy) {
        this.resetStrategy = resetStrategy;
        return this;
    }

    public OffsetInitializationType withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public OffsetsInitializer toOffsetsInitializer() {
        switch (this) {
            case COMMITTED_WITH_RESET_STRATEGY:
                return resetStrategy != null ?
                        OffsetsInitializer.committedOffsets(resetStrategy) :
                        OffsetsInitializer.committedOffsets();
            case TIMESTAMP:
                return OffsetsInitializer.timestamp(timestamp);
            case EARLIEST:
                return OffsetsInitializer.earliest();
            case LATEST:
                return OffsetsInitializer.latest();
            default:
                throw new IllegalArgumentException("Invalid offset initialization type: " + this + ". " +
                        "Acceptable values are COMMITTED_WITH_RESET_STRATEGY, TIMESTAMP, EARLIEST, LATEST.");
        }
    }
}
