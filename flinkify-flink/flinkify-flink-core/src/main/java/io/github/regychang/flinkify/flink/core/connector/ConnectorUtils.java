package io.github.regychang.flinkify.flink.core.connector;

import io.github.regychang.flinkify.common.config.ConfigOption;
import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ConfigurationException;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.config.ConnectorOptions;
import io.github.regychang.flinkify.flink.core.config.FlinkOptions;
import io.github.regychang.flinkify.flink.core.config.SinkConnectorOptions;
import io.github.regychang.flinkify.flink.core.config.SourceConnectorOptions;
import io.github.regychang.flinkify.flink.core.connector.csv.config.CsvOptions;
import io.github.regychang.flinkify.flink.core.connector.csv.source.CsvSourceConnector;
import io.github.regychang.flinkify.flink.core.connector.datagen.config.DataGenOptions;
import io.github.regychang.flinkify.flink.core.connector.datagen.source.DataGenSourceConnector;
import io.github.regychang.flinkify.flink.core.connector.doris.config.DorisOptions;
import io.github.regychang.flinkify.flink.core.connector.doris.sink.DorisSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.doris.source.DorisSourceConnector;
import io.github.regychang.flinkify.flink.core.connector.kafka.config.KafkaOptions;
import io.github.regychang.flinkify.flink.core.connector.kafka.sink.KafkaSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.kafka.source.KafkaSourceConnector;
import io.github.regychang.flinkify.flink.core.connector.mongo.config.MongoOptions;
import io.github.regychang.flinkify.flink.core.connector.mongo.sink.MongoSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.mysql.cdc.MySqlCdcConnector;
import io.github.regychang.flinkify.flink.core.connector.mysql.config.MySqlOptions;
import io.github.regychang.flinkify.flink.core.connector.nebula.config.NebulaOptions;
import io.github.regychang.flinkify.flink.core.connector.nebula.sink.NebulaSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.nebula.source.NebulaSourceConnector;
import io.github.regychang.flinkify.flink.core.connector.oracle.cdc.OracleCdcConnector;
import io.github.regychang.flinkify.flink.core.connector.oracle.config.OracleOptions;
import io.github.regychang.flinkify.flink.core.connector.postgres.cdc.PostgresCdcConnector;
import io.github.regychang.flinkify.flink.core.connector.postgres.config.PostgresOptions;
import io.github.regychang.flinkify.flink.core.connector.postgres.sink.PostgresSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.sink.RabbitmqSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.rabbitmq.source.RabbitmqSourceConnector;

import io.github.regychang.flinkify.flink.core.connector.sqlserver.config.SqlServerOptions;
import io.github.regychang.flinkify.flink.core.connector.sqlserver.sink.SqlServerSinkConnector;
import io.github.regychang.flinkify.flink.core.connector.sqlserver.source.SqlServerCdcConnector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class ConnectorUtils {

    public static Map<ConnectorKey<?>, SourceConnector<?>> initSourceConnectors(
            StreamExecutionEnvironment env, Configuration appConfig) {

        List<ConnectorType<SourceConnector<?>>> sourceConnectorTypes =
                Arrays.asList(
                        new ConnectorType<>(KafkaOptions.CONNECTORS, KafkaSourceConnector::new),
                        new ConnectorType<>(
                                RabbitmqOptions.CONNECTORS, RabbitmqSourceConnector::new),
                        new ConnectorType<>(NebulaOptions.CONNECTORS, NebulaSourceConnector::new),
                        new ConnectorType<>(DorisOptions.CONNECTORS, DorisSourceConnector::new),
                        new ConnectorType<>(CsvOptions.CONNECTORS, CsvSourceConnector::new),
                        new ConnectorType<>(MySqlOptions.CDC, MySqlCdcConnector::new),
                        new ConnectorType<>(OracleOptions.CDC, OracleCdcConnector::new),
                        new ConnectorType<>(PostgresOptions.CDC, PostgresCdcConnector::new),
                        new ConnectorType<>(SqlServerOptions.CDC, SqlServerCdcConnector::new),
                        new ConnectorType<>(
                                DataGenOptions.CONNECTORS, DataGenSourceConnector::new));

        return initConnectors(env, appConfig, sourceConnectorTypes, FlinkOptions.SOURCE_CONNECTORS);
    }

    public static Map<ConnectorKey<?>, SinkConnector<?, ?>> initSinkConnectors(
            StreamExecutionEnvironment env, Configuration config) {

        List<ConnectorType<SinkConnector<?, ?>>> sinkConnectorTypes =
                Arrays.asList(
                        new ConnectorType<>(KafkaOptions.CONNECTORS, KafkaSinkConnector::new),
                        new ConnectorType<>(RabbitmqOptions.CONNECTORS, RabbitmqSinkConnector::new),
                        new ConnectorType<>(NebulaOptions.CONNECTORS, NebulaSinkConnector::new),
                        new ConnectorType<>(MongoOptions.CONNECTORS, MongoSinkConnector::new),
                        new ConnectorType<>(DorisOptions.CONNECTORS, DorisSinkConnector::new),
                        new ConnectorType<>(
                                SqlServerOptions.CONNECTORS, SqlServerSinkConnector::new),
                        new ConnectorType<>(
                                PostgresOptions.CONNECTORS, PostgresSinkConnector::new));
        //                        new ConnectorType<>(TelegramOptions.CONNECTORS,
        // TelegramSinkConnector::new));

        return initConnectors(env, config, sinkConnectorTypes, FlinkOptions.SINK_CONNECTORS);
    }

    private static <T extends Connector> Map<ConnectorKey<?>, T> initConnectors(
            StreamExecutionEnvironment env,
            Configuration appConfig,
            List<ConnectorType<T>> connectorTypes,
            ConfigOption<List<Configuration>> connectorConfigOption) {

        List<Configuration> connectorConfigs = appConfig.get(connectorConfigOption);

        return connectorConfigs.stream()
                .flatMap(
                        connectorConfig -> {
                            Optional<ConnectorType<T>> matchingConnectorType =
                                    connectorTypes.stream()
                                            .filter(
                                                    type ->
                                                            connectorConfig.contains(
                                                                    type.configOption))
                                            .findFirst();

                            if (matchingConnectorType.isEmpty()) {
                                throw new ConfigurationException(
                                        ErrCode.PARSING_CONFIG_FAILED,
                                        String.format(
                                                "Unknown connector type '%s',"
                                                        + " please check your configuration of connector",
                                                connectorConfig.toMap().keySet()));
                            }

                            List<Configuration> configs =
                                    connectorConfig.getNotNull(
                                            matchingConnectorType.get().configOption,
                                            "Could not find configuration");

                            return configs.stream()
                                    .map(
                                            config -> {
                                                TypeInformation<?> typeInformation =
                                                        connectorConfigOption
                                                                        .getKey()
                                                                        .equals("flink.sources")
                                                                ? config.getNotNull(
                                                                        SourceConnectorOptions
                                                                                .DATA_TYPE,
                                                                        "Could not find data type of source connector")
                                                                : config.getNotNull(
                                                                        SinkConnectorOptions
                                                                                .INPUT_DATA_TYPE,
                                                                        "Could not find data type of sink connector");

                                                ConnectorKey<?> key =
                                                        new ConnectorKey<>(
                                                                config.get(ConnectorOptions.ID),
                                                                typeInformation);
                                                T value =
                                                        matchingConnectorType
                                                                .get()
                                                                .connectorFactory
                                                                .apply(env, config);

                                                return new AbstractMap.SimpleEntry<>(key, value);
                                            });
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static class ConnectorType<T extends Connector> {
        private final ConfigOption<List<Configuration>> configOption;
        private final BiFunction<StreamExecutionEnvironment, Configuration, T> connectorFactory;

        ConnectorType(
                ConfigOption<List<Configuration>> configOption,
                BiFunction<StreamExecutionEnvironment, Configuration, T> connectorFactory) {
            this.configOption = configOption;
            this.connectorFactory = connectorFactory;
        }
    }
}
