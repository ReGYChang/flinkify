package com.regy.quantalink.flink.core.connector.nebula.source;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public class NebulaSourceConnector<T> extends SourceConnector<T> {

    public NebulaSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        return null;
    }
}
