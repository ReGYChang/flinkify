package io.github.regychang.flinkify.flink.core.streaming;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.config.ConfigurationUtils;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.flink.core.config.FlinkOptions;
import io.github.regychang.flinkify.flink.core.connector.ConnectorUtils;

import lombok.Getter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Getter
public abstract class FlinkStreaming {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreaming.class);

    protected FlinkStreamingContext context;

    protected void init(String[] args) {
        Configuration config = loadConfig(args);
        org.apache.flink.configuration.Configuration flinkConf = config.get(FlinkOptions.CONFIG).toFlinkConfig();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);

        this.context =
                new FlinkStreamingContext.Builder()
                        .withEnv(env)
                        .withTEnv(StreamTableEnvironment.create(env))
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
        JobExecutionResult executionRes =
                context.getEnv().execute(
                        context.getConfigOption(FlinkOptions.JOB_NAME));

        LOG.info("[QuantaLink]: Terminating the Flink job");
        terminate();

        LOG.info("[QuantaLink]: Flink job finished with result: {}", executionRes);
    }

    private Configuration loadConfig(String[] args) {
        Optional<String> confPathOpt = Optional.ofNullable(ParameterTool.fromArgs(args).get("conf"));
        return confPathOpt.map(ConfigurationUtils::loadYamlConfigFromPath)
                .orElseGet(
                        () ->
                                ConfigurationUtils.loadYamlConfigFromClasspath(
                                        this.getClass().getClassLoader(),
                                        confPathOpt.orElse("application.yml")));
    }
}
