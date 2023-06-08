package com.regy.quantalink.flink.core.connector.doris.source;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SourceConnector;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author regy
 */
public class DorisSourceConnector<T> extends SourceConnector<T> {

    public DorisSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        //TODO
        return null;
    }
}
