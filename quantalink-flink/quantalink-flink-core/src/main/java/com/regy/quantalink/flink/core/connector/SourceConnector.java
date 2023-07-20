package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.SourceConnectorOptions;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * @author regy
 */
public abstract class SourceConnector<T> extends Connector {

    private DeserializationAdapter<T, ?> deserializationAdapter;
    private WatermarkStrategy<T> watermarkStrategy;
    private final TypeInformation<T> typeInfo;

    @SuppressWarnings("unchecked")
    public SourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.typeInfo = (TypeInformation<T>) config.getNotNull(SourceConnectorOptions.DATA_TYPE, String.format("Source connector '%s' data type must not be null", getName()));
    }

    /**
     * Creates a data stream from the source.
     * Subclasses must provide the specific implementation.
     */
    public abstract DataStreamSource<T> getSourceDataStream() throws FlinkException;

    public DeserializationAdapter<T, ?> getDeserializationAdapter() {
        return deserializationAdapter;
    }

    public WatermarkStrategy<T> getWatermarkStrategy() {
        return Optional.ofNullable(watermarkStrategy).orElse(WatermarkStrategy.noWatermarks());
    }

    public TypeInformation<T> getTypeInfo() {
        return typeInfo;
    }

    public SourceConnector<T> withDeserializationSchemaAdapter(DeserializationAdapter<T, ?> deserializationAdapter) {
        this.deserializationAdapter = deserializationAdapter;
        return this;
    }

    public SourceConnector<T> withWatermarkStrategy(WatermarkStrategy<T> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }
}

