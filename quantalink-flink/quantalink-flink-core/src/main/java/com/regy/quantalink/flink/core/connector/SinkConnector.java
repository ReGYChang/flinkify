package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.serialization.SerializationAdapter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public abstract class SinkConnector<T> extends Connector<T> {

    protected SerializationAdapter<T, ?> serializationAdapter;

    public SinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
    }

    /**
     * Creates a data stream from the sink.
     * Subclasses must provide the specific implementation.
     */
    public abstract DataStreamSink<T> getSinkDataStream(DataStream<T> stream);

    public void withSerializationAdapter(SerializationAdapter<T, ?> serializationAdapter) {
        this.serializationAdapter = serializationAdapter;
    }
}
