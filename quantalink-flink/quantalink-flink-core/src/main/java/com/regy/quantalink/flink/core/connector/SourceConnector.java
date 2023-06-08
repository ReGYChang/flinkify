package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public abstract class SourceConnector<T> extends Connector<T> {

    protected DeserializationAdapter<T> deserializationAdapter;
    protected WatermarkStrategy<T> watermarkStrategy;

    public SourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
    }

    /**
     * Creates a data stream from the source.
     * Subclasses must provide the specific implementation.
     */
    public abstract DataStreamSource<T> getSourceDataStream() throws FlinkException;

    public SourceConnector<T> withDeserializationSchemaAdapter(DeserializationAdapter<T> deserializationAdapter) {
        this.deserializationAdapter = deserializationAdapter;
        return this;
    }

    public SourceConnector<T> withWatermarkStrategy(WatermarkStrategy<T> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }
}

