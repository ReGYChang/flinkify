package com.regy.quantalink.flink.core.connector;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ConfigurationException;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.FlinkOptions;
import com.regy.quantalink.flink.core.config.SinkConnectorOptions;
import com.regy.quantalink.flink.core.config.SourceConnectorOptions;
import com.regy.quantalink.flink.core.connector.doris.config.DorisOptions;
import com.regy.quantalink.flink.core.connector.doris.sink.DorisSinkConnector;
import com.regy.quantalink.flink.core.connector.doris.source.DorisSourceConnector;
import com.regy.quantalink.flink.core.connector.kafka.config.KafkaOptions;
import com.regy.quantalink.flink.core.connector.kafka.sink.KafkaSinkConnector;
import com.regy.quantalink.flink.core.connector.kafka.source.KafkaSourceConnector;
import com.regy.quantalink.flink.core.connector.mongo.config.MongoOptions;
import com.regy.quantalink.flink.core.connector.mongo.sink.MongoSinkConnector;
import com.regy.quantalink.flink.core.connector.mysql.cdc.MySqlCdcConnector;
import com.regy.quantalink.flink.core.connector.mysql.config.MySqlOptions;
import com.regy.quantalink.flink.core.connector.nebula.config.NebulaOptions;
import com.regy.quantalink.flink.core.connector.nebula.sink.NebulaSinkConnector;
import com.regy.quantalink.flink.core.connector.nebula.source.NebulaSourceConnector;
import com.regy.quantalink.flink.core.connector.oracle.cdc.OracleCdcConnector;
import com.regy.quantalink.flink.core.connector.oracle.config.OracleOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.config.RabbitmqOptions;
import com.regy.quantalink.flink.core.connector.rabbitmq.sink.RabbitmqSinkConnector;
import com.regy.quantalink.flink.core.connector.rabbitmq.source.RabbitmqSourceConnector;
import com.regy.quantalink.flink.core.connector.telegram.config.TelegramOptions;
import com.regy.quantalink.flink.core.connector.telegram.sink.TelegramSinkConnector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.TriFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author regy
 */
public class ConnectorUtils {

    public static Map<TypeInformation<?>, SourceConnector<?>> initSourceConnectors(
            StreamExecutionEnvironment env,
            Configuration appConfig) {

        return initConnectors(env, appConfig, true,
                (environment, connectorConfig, connectors) -> {
                    if (connectorConfig.contains(KafkaOptions.CONNECTORS)) {
                        List<Configuration> kafkaConfigs = connectorConfig.getNotNull(KafkaOptions.CONNECTORS, "Could not find configuration of kafka source connector");
                        kafkaConfigs.forEach(
                                kafkaConfig ->
                                        connectors.put(kafkaConfig.getNotNull(SourceConnectorOptions.DATA_TYPE, "Could not find data type of kafka source connector"), new KafkaSourceConnector<>(environment, kafkaConfig)));
                    } else if (connectorConfig.contains(RabbitmqOptions.CONNECTORS)) {
                        List<Configuration> rabbitmqConfigs = connectorConfig.getNotNull(RabbitmqOptions.CONNECTORS, "Could not find configuration of RabbitMQ source connector");
                        rabbitmqConfigs.forEach(
                                rabbitmqConfig ->
                                        connectors.put(rabbitmqConfig.getNotNull(SourceConnectorOptions.DATA_TYPE, "Could not find data type of RabbitMQ source connector"), new RabbitmqSourceConnector<>(environment, rabbitmqConfig)));
                    } else if (connectorConfig.contains(NebulaOptions.CONNECTORS)) {
                        List<Configuration> nebulaConfigs = connectorConfig.getNotNull(NebulaOptions.CONNECTORS, "Could not find configuration of nebula graph source connector");
                        nebulaConfigs.forEach(
                                nebulaConfig ->
                                        connectors.put(nebulaConfig.getNotNull(SourceConnectorOptions.DATA_TYPE, "Could not find data type of nebula graph source connector"), new NebulaSourceConnector<>(environment, nebulaConfig)));
                    } else if (connectorConfig.contains(DorisOptions.CONNECTORS)) {
                        List<Configuration> dorisConfigs = connectorConfig.getNotNull(DorisOptions.CONNECTORS, "Could not find configuration of doris source connector");
                        dorisConfigs.forEach(
                                dorisConfig ->
                                        connectors.put(dorisConfig.getNotNull(SourceConnectorOptions.DATA_TYPE, "Could not find data type of doris source connector"), new DorisSourceConnector(environment, dorisConfig)));
                    } else if (connectorConfig.contains(MySqlOptions.CDC)) {
                        Configuration mysqlCdcConfig = connectorConfig.getNotNull(MySqlOptions.CDC, "Could not find configuration of mysql cdc connector");
                        mysqlCdcConfig.set(SourceConnectorOptions.DATA_TYPE, TypeInformation.get(String.class));
                        connectors.put(TypeInformation.get(String.class), new MySqlCdcConnector(environment, mysqlCdcConfig));
                    } else if (connectorConfig.contains(OracleOptions.CDC)) {
                        Configuration oracleCdcConfig = connectorConfig.getNotNull(OracleOptions.CDC, "Could not find configuration of oracle cdc connector");
                        oracleCdcConfig.set(SourceConnectorOptions.DATA_TYPE, TypeInformation.get(String.class));
                        connectors.put(TypeInformation.get(String.class), new OracleCdcConnector(environment, oracleCdcConfig));
                    } else {
                        throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, String.format("Unknown source connector type '%s', please check your configuration of connector", connectorConfig.toMap().keySet()));
                    }
                    return null;
                });
    }

    public static Map<TypeInformation<?>, SinkConnector<?, ?>> initSinkConnectors(
            StreamExecutionEnvironment env,
            Configuration config) {

        return initConnectors(env, config, false,
                (environment, connectorConfig, connectors) -> {
                    if (connectorConfig.contains(KafkaOptions.CONNECTORS)) {
                        List<Configuration> kafkaConfigs = connectorConfig.getNotNull(KafkaOptions.CONNECTORS, "Could not find configuration of kafka sink connector");
                        kafkaConfigs.forEach(
                                kafkaConfig ->
                                        connectors.put(kafkaConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not find input data type of kafka sink connector"), new KafkaSinkConnector<>(environment, kafkaConfig)));
                    } else if (connectorConfig.contains(RabbitmqOptions.CONNECTORS)) {
                        List<Configuration> rabbitmqConfigs = connectorConfig.getNotNull(RabbitmqOptions.CONNECTORS, "Could not find configuration of RabbitMQ sink connector");
                        rabbitmqConfigs.forEach(
                                rabbitmqConfig ->
                                        connectors.put(rabbitmqConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not find input data type of RabbitMQ sink connector"), new RabbitmqSinkConnector<>(environment, rabbitmqConfig)));
                    } else if (connectorConfig.contains(NebulaOptions.CONNECTORS)) {
                        List<Configuration> nebulaConfigs = connectorConfig.getNotNull(NebulaOptions.CONNECTORS, "Could not find configuration of nebula graph sink connector");
                        nebulaConfigs.forEach(
                                nebulaConfig ->
                                        connectors.put(nebulaConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not input find data type of nebula graph sink connector"), new NebulaSinkConnector<>(environment, nebulaConfig)));
                    } else if (connectorConfig.contains(MongoOptions.CONNECTORS)) {
                        List<Configuration> mongoConfigs = connectorConfig.getNotNull(MongoOptions.CONNECTORS, "Could not find configuration of mongo sink connector");
                        mongoConfigs.forEach(
                                mongoConfig ->
                                        connectors.put(mongoConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not input find data type of mongo sink connector"), new MongoSinkConnector<>(environment, mongoConfig)));
                    } else if (connectorConfig.contains(DorisOptions.CONNECTORS)) {
                        List<Configuration> dorisConfigs = connectorConfig.getNotNull(DorisOptions.CONNECTORS, "Could not find configuration of doris sink connector");
                        dorisConfigs.forEach(
                                dorisConfig ->
                                        connectors.put(dorisConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not find input data type of doris sink connector"), new DorisSinkConnector<>(environment, dorisConfig)));
                    } else if (connectorConfig.contains(TelegramOptions.CONNECTORS)) {
                        List<Configuration> telegramConfigs = connectorConfig.getNotNull(TelegramOptions.CONNECTORS, "Could not find configuration of telegram sink connector");
                        telegramConfigs.forEach(
                                telegramConfig ->
                                        connectors.put(telegramConfig.getNotNull(SinkConnectorOptions.INPUT_DATA_TYPE, "Could not find input data type of telegram sink connector"), new TelegramSinkConnector<>(environment, telegramConfig)));
                    } else {
                        throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, String.format("Unknown sink connector type %s, please check your configuration of connector", connectorConfig.toMap().keySet()));
                    }
                    return null;
                });
    }

    private static <T extends Connector> Map<TypeInformation<?>, T> initConnectors(
            StreamExecutionEnvironment env,
            Configuration appConfig,
            boolean isSource,
            TriFunction<StreamExecutionEnvironment, Configuration, Map<TypeInformation<?>, T>, Void> connectorInitFunc) {

        HashMap<TypeInformation<?>, T> connectors = new HashMap<>();
        List<Configuration> connectorConfigs = appConfig.get(isSource ? FlinkOptions.SOURCE_CONNECTORS : FlinkOptions.SINK_CONNECTORS);
        connectorConfigs.forEach(connectorConfig -> connectorInitFunc.apply(env, connectorConfig, connectors));

        return connectors;
    }
}
