package com.regy.quantalink.flink.core.streaming;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.config.ConfigurationUtils;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.config.FlinkOptions;
import com.regy.quantalink.flink.core.connector.ConnectorUtils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author regy
 */
public abstract class FlinkStreaming {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreaming.class);

    protected FlinkStreamingContext context;

    protected void init(String[] args) {
        Configuration config = ConfigurationUtils.loadYamlConfigFromParameters(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.context =
                new FlinkStreamingContext.Builder()
                        .withEnv(env)
                        .withConfig(config)
                        .withSourceConnectors(ConnectorUtils.initSourceConnectors(env, config))
                        .withSinkConnectors(ConnectorUtils.initSinkConnectors(env, config))
                        .build();
    }

    protected void config(FlinkStreamingInitializer initializer) throws FlinkException {
        initializer.init(context);
    }

    protected abstract void execute(FlinkStreamingContext context) throws FlinkException;

    protected void terminate() throws FlinkException {
    }

    protected void run(String[] args, FlinkStreamingInitializer... initializer) throws Exception {
        LOG.info("[QuantaLink]: Initializing the Flink job");
        init(args);

        if (initializer.length > 0) {
            LOG.info("[QuantaLink]: Setting configuration of Flink job");
            config(initializer[0]);
        }

        LOG.info("[QuantaLink]: Executing the Flink job");
        execute(context);

        LOG.info("[QuantaLink]: Running the Flink job");
        JobExecutionResult executionRes = context.getEnv().execute(context.getConfigOption(FlinkOptions.JOB_NAME));

        LOG.info("[QuantaLink]: Terminating the Flink job");
        terminate();

        LOG.info("[QuantaLink]: Flink job finished with result: {}", executionRes);
    }
}

