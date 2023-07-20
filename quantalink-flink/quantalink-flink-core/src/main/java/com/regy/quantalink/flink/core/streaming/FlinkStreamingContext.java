package com.regy.quantalink.flink.core.streaming;

import com.regy.quantalink.common.config.ConfigOption;
import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.SourceConnector;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author regy
 */
public class FlinkStreamingContext {
    private final StreamExecutionEnvironment env;
    private final StreamTableEnvironment tEnv;
    private final Configuration config;
    private final String jobName;
    private final JobID jobId;
    private final MetricGroup metricGroup;
    private final Map<String, AutoCloseable> resources;
    private final Map<TypeInformation<?>, SourceConnector<?>> sourceConnectors;
    private final Map<TypeInformation<?>, SinkConnector<?, ?>> sinkConnectors;

    private FlinkStreamingContext(Builder builder) {
        this.env = builder.env;
        this.config = builder.config;
        this.jobName = builder.jobName;
        this.jobId = builder.jobId;
        this.metricGroup = builder.metricGroup;
        this.resources = builder.resources;
        this.sourceConnectors = builder.sourceConnectors;
        this.sinkConnectors = builder.sinkConnectors;
        this.tEnv = builder.tEnv;
    }

    @SuppressWarnings("unchecked")
    public <T> SourceConnector<T> getSourceConnector(TypeInformation<T> typeInformation) {
        return Optional.ofNullable((SourceConnector<T>) sourceConnectors.get(typeInformation))
                .orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Source connector could not be found with type: [%s]", typeInformation)));
    }

    @SuppressWarnings("unchecked")
    public <I, O> SinkConnector<I, O> getSinkConnector(TypeInformation<I> inputTypeInfo, TypeInformation<O> outputTypeInfo) {
        return Optional.ofNullable((SinkConnector<I, O>) sinkConnectors.get(inputTypeInfo))
                .or(
                        () ->
                                Optional.ofNullable((SinkConnector<I, O>) sinkConnectors.get(outputTypeInfo)))
                .orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Sink connector could not be found with input type '%s' & output type '%s' ", inputTypeInfo, outputTypeInfo)));
    }

    public <T> DataStreamSource<T> getSourceDataStream(TypeInformation<T> typeInformation) throws FlinkException {
        return Optional.ofNullable(this.getSourceConnector(typeInformation).getSourceDataStream())
                .orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Source data stream could not be found with type: [%s]", typeInformation)));
    }

    @SuppressWarnings("UnusedReturnValue")
    public <I, O> DataStreamSink<O> getSinkDataStream(TypeInformation<I> inputTypeInfo, TypeInformation<O> outputTypeInfo, DataStream<I> stream) {
        SinkConnector<I, O> sinkConnector = this.getSinkConnector(inputTypeInfo, outputTypeInfo);
        return Optional.ofNullable(sinkConnector.getSinkDataStream(stream))
                .orElseThrow(() -> new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Sink connector could not be found with input type '%s' & output type '%s' ", inputTypeInfo, outputTypeInfo)));
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTEnv() {
        return tEnv;
    }

    public Configuration getConfig() {
        return config;
    }

    public <T> T getConfigOption(ConfigOption<T> option) {
        return config.get(option);
    }

    public JobID getJobId() {
        return jobId;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public Map<String, AutoCloseable> getResources() {
        return resources;
    }

    public Map<TypeInformation<?>, SourceConnector<?>> getSourceConnectors() {
        return sourceConnectors;
    }

    public Map<TypeInformation<?>, SinkConnector<?, ?>> getSinkConnectors() {
        return sinkConnectors;
    }

    public static class Builder {
        private StreamExecutionEnvironment env;
        private StreamTableEnvironment tEnv;
        private Configuration config;
        private String jobName;
        private JobID jobId;
        private MetricGroup metricGroup;
        private final Map<String, AutoCloseable> resources = new HashMap<>();
        private Map<TypeInformation<?>, SourceConnector<?>> sourceConnectors;
        private Map<TypeInformation<?>, SinkConnector<?, ?>> sinkConnectors;

        public Builder withEnv(StreamExecutionEnvironment env) {
            this.env = env;
            return this;
        }

        public Builder withTEnv(StreamTableEnvironment tEnv) {
            this.tEnv = tEnv;
            return this;
        }

        public Builder withConfig(Configuration config) {
            this.config = config;
            return this;
        }

        public Builder withJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder withJobId(JobID jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder withMetricGroup(MetricGroup metricGroup) {
            this.metricGroup = metricGroup;
            return this;
        }

        public Builder withResource(String key, AutoCloseable resource) {
            this.resources.put(key, resource);
            return this;
        }

        public Builder withSourceConnectors(Map<TypeInformation<?>, SourceConnector<?>> sourceConnectors) {
            this.sourceConnectors = sourceConnectors;
            return this;
        }

        public Builder withSinkConnectors(Map<TypeInformation<?>, SinkConnector<?, ?>> sinkConnectors) {
            this.sinkConnectors = sinkConnectors;
            return this;
        }

        public FlinkStreamingContext build() {
            return new FlinkStreamingContext(this);
        }
    }
}
