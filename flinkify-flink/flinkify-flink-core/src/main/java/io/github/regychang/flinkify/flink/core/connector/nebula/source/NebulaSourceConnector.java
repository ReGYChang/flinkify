package io.github.regychang.flinkify.flink.core.connector.nebula.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NebulaSourceConnector<T> extends SourceConnector<T> {

    public NebulaSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
    }

    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        return null;
    }
}
