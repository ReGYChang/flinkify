package io.github.regychang.flinkify.flink.core.streaming;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ConfigurationException;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.exception.QuantalinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.SinkConnector;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

@FunctionalInterface
public interface FlinkStreamingInitializer {

    void init(FlinkStreamingContext context) throws QuantalinkException;

    class Builder {

        private final List<FlinkStreamingInitializer> initializers = new ArrayList<>();

        public Builder withExecutionEnvironmentSetup(ExecutionEnvironmentInitializer initializer) {
            initializers.add(
                    (ctx) -> {
                        try {
                            initializer.init(ctx.getEnv());
                        } catch (Exception e) {
                            throw new FlinkException(
                                    ErrCode.STREAMING_ENV_FAILED,
                                    "Failed to initialize Flink execution environment", e);
                        }
                    });
            return this;
        }

        public Builder withConfigurationSetup(ConfigurationInitializer initializer) {
            initializers.add(
                    (ctx) -> {
                        try {
                            initializer.init(ctx.getConfig());
                        } catch (Exception e) {
                            throw new ConfigurationException(
                                    ErrCode.STREAMING_CONFIG_FAILED,
                                    "Failed to initialize Flink configuration", e);
                        }
                    });
            return this;
        }

        public <T> Builder withSourceConnectorSetup(
                SourceConnectorInitializer<T> initializer, TypeInformation<T> typeInformation) {
            initializers.add(
                    (ctx) -> {
                        try {
                            SourceConnector<T> sourceConnector = ctx.getSourceConnector(typeInformation);
                            initializer.init(sourceConnector, sourceConnector.getConfig());
                        } catch (Exception e) {
                            throw new ConfigurationException(
                                    ErrCode.STREAMING_CONFIG_FAILED,
                                    String.format("Failed to initialize Flink source connector '%s'", typeInformation),
                                    e);
                        }
                    });
            return this;
        }

        public <T> Builder withSinkConnectorSetup(
                SinkConnectorInitializer<T, T> initializer, TypeInformation<T> typeInfo) {
            return withSinkConnectorSetup(initializer, typeInfo, typeInfo);
        }

        public <I, O> Builder withSinkConnectorSetup(
                SinkConnectorInitializer<I, O> initializer,
                TypeInformation<I> inputTypeInfo,
                TypeInformation<O> outputTypeInfo) {
            initializers.add(
                    (ctx) -> {
                        try {
                            SinkConnector<I, O> sinkConnector = ctx.getSinkConnector(inputTypeInfo, outputTypeInfo);
                            initializer.init(sinkConnector, sinkConnector.getConfig());
                        } catch (Exception e) {
                            throw new ConfigurationException(
                                    ErrCode.STREAMING_CONFIG_FAILED,
                                    String.format("Failed to initialize Flink sink connector '%s'", inputTypeInfo),
                                    e);
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
