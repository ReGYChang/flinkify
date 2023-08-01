package com.regy.quantalink.flink.core.streaming;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ConfigurationException;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.exception.QuantalinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.SourceConnector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author regy
 */
@FunctionalInterface
public interface FlinkStreamingInitializer {

    void init(FlinkStreamingContext context) throws QuantalinkException;

    class Builder {
        private final List<FlinkStreamingInitializer> initializers = new ArrayList<>();

        public Builder withExecutionEnvironmentSetup(ExecutionEnvironmentInitializer initializer) {
            initializers.add((ctx) -> {
                try {
                    initializer.init(ctx.getEnv());
                } catch (Exception e) {
                    throw new FlinkException(ErrCode.STREAMING_ENV_FAILED, "Failed to initialize Flink execution environment", e);
                }
            });
            return this;
        }

        public Builder withConfigurationSetup(ConfigurationInitializer initializer) {
            initializers.add((ctx) -> {
                try {
                    initializer.init(ctx.getConfig());
                } catch (Exception e) {
                    throw new ConfigurationException(ErrCode.STREAMING_CONFIG_FAILED, "Failed to initialize Flink configuration", e);
                }
            });
            return this;
        }

        public <T> Builder withSourceConnectorSetup(SourceConnectorInitializer<T> initializer, TypeInformation<T> typeInformation) {
            initializers.add(
                    (ctx) -> {
                        try {
                            SourceConnector<T> sourceConnector = Optional.ofNullable(ctx.getSourceConnector(typeInformation))
                                    .orElseThrow(() -> new ConfigurationException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Source connector '%s' not found, please check your connector data-type in the configuration", typeInformation)));
                            Configuration config = Optional.ofNullable(ctx.getConfig())
                                    .orElseThrow(() -> new ConfigurationException(ErrCode.MISSING_CONFIG_FILE, "Config file not found"));
                            initializer.init(sourceConnector, config);
                        } catch (Exception e) {
                            throw new ConfigurationException(ErrCode.STREAMING_CONFIG_FAILED, String.format("Failed to initialize Flink source connector '%s'", typeInformation), e);
                        }
                    });
            return this;
        }

        public <T> Builder withSinkConnectorSetup(SinkConnectorInitializer<T, T> initializer, TypeInformation<T> typeInfo) {
            return withSinkConnectorSetup(initializer, typeInfo, typeInfo);
        }

        public <I, O> Builder withSinkConnectorSetup(SinkConnectorInitializer<I, O> initializer, TypeInformation<I> inputTypeInfo, TypeInformation<O> outputTypeInfo) {
            initializers.add(
                    (ctx) -> {
                        try {
                            SinkConnector<I, O> sinkConnector = Optional.ofNullable(ctx.getSinkConnector(inputTypeInfo, outputTypeInfo))
                                    .orElseThrow(() -> new ConfigurationException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Sink connector not found with input type '%s' and output type '%s', please check your connector data-type in the configuration", inputTypeInfo, outputTypeInfo)));
                            Configuration config = Optional.ofNullable(ctx.getConfig())
                                    .orElseThrow(() -> new ConfigurationException(ErrCode.MISSING_CONFIG_FILE, "Config file not found"));
                            initializer.init(sinkConnector, config);
                        } catch (Exception e) {
                            throw new ConfigurationException(ErrCode.STREAMING_CONFIG_FAILED, String.format("Failed to initialize Flink sink connector '%s'", inputTypeInfo), e);
                        }
                    });
            return this;
        }

        public FlinkStreamingInitializer build() {
            return (ctx) -> {
                for (FlinkStreamingInitializer initializer : initializers) {
                    initializer.init(ctx);
                }
            };
        }
    }

    @FunctionalInterface
    interface ExecutionEnvironmentInitializer {
        void init(StreamExecutionEnvironment env) throws Exception;
    }

    @FunctionalInterface
    interface ConfigurationInitializer {
        void init(Configuration config) throws Exception;
    }

    @FunctionalInterface
    interface SourceConnectorInitializer<T> {
        void init(SourceConnector<T> sourceConnector, Configuration config) throws Exception;
    }

    @FunctionalInterface
    interface SinkConnectorInitializer<I, O> {
        void init(SinkConnector<I, O> sinkConnector, Configuration config) throws Exception;
    }

    // You can define similar interfaces for other initialization phases...
}
