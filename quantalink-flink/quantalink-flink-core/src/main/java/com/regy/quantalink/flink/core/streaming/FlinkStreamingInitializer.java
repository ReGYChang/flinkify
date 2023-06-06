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
import java.util.Map;

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
            initializers.add((ctx) -> {
                try {
                    initializer.init(ctx.getSourceConnector(typeInformation));
                } catch (Exception e) {
                    throw new ConfigurationException(ErrCode.STREAMING_CONFIG_FAILED, "Failed to initialize Flink source connectors", e);
                }
            });
            return this;
        }

        public Builder withSinkConnectorSetup(SinkConnectorInitializer initializer) {
            initializers.add((ctx) -> {
                try {
                    initializer.init(ctx.getSinkConnectors());
                } catch (Exception e) {
                    throw new ConfigurationException(ErrCode.STREAMING_CONFIG_FAILED, "Failed to initialize Flink sink connectors", e);
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
        void init(SourceConnector<T> sourceConnector) throws Exception;
    }

    @FunctionalInterface
    interface SinkConnectorInitializer {
        void init(Map<TypeInformation<?>, SinkConnector<?>> sinkConnectors) throws Exception;
    }

    // You can define similar interfaces for other initialization phases...
}
