package com.regy.quantalink.flink.core.connector.mongo.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.mongo.config.MongoOptions;
import com.regy.quantalink.flink.core.connector.mongo.serialization.MongoSerializationAdapter;

import com.alibaba.fastjson2.JSON;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;

import java.util.Optional;

/**
 * @author regy
 */
public class MongoSinkConnector<T> extends SinkConnector<T> {

    private final String uri;
    private final String database;
    private final String collection;
    private final Integer batchSize;
    private final Long batchIntervalMs;
    private final Integer maxRetries;

    public MongoSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.uri = config.getNotNull(MongoOptions.URI, "Mongo sink connector uri must not be null, please check your configuration");
        this.database = config.getNotNull(MongoOptions.DATABASE, "Mongo sink connector database must not be null, please check your configuration");
        this.collection = config.getNotNull(MongoOptions.COLLECTION, "Mongo sink connector collection must not be null, please check your configuration");
        this.batchSize = config.getNotNull(MongoOptions.BATCH_SIZE, "Mongo sink connector batch-size must not be null, please check your configuration");
        this.batchIntervalMs = config.getNotNull(MongoOptions.BATCH_INTERVAL_MS, "Mongo sink connector batch-interval-ms must not be null, please check your configuration");
        this.maxRetries = config.getNotNull(MongoOptions.MAX_RETRIES, "Mongo sink connector max-retries must not be null, please check your configuration");
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        try {
            MongoSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((MongoSerializationAdapter<T>) super.serializationAdapter).orElse(
                            new MongoSerializationAdapter<>(
                                    (input, cxt) ->
                                            new InsertOneModel<>(BsonDocument.parse(JSON.toJSONString(input))), super.typeInfo));
            MongoSink<T> sink = MongoSink.<T>builder()
                    .setUri(uri)
                    .setDatabase(database)
                    .setCollection(collection)
                    .setBatchSize(batchSize)
                    .setBatchIntervalMs(batchIntervalMs)
                    .setMaxRetries(maxRetries)
                    .setSerializationSchema(serializationAdapter.getSerializationSchema())
                    .build();
            return stream.sinkTo(sink).name(super.name).setParallelism(super.parallelism).disableChaining();
        } catch (ClassCastException e1) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("MongoDB sink connector '%s' com.nexdata.flink.traceability.serialization adapter must be '%s', could not assign other com.nexdata.flink.traceability.serialization adapter", name, MongoSerializationAdapter.class), e1);
        } catch (Exception e2) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to initialize MongoDB sink connector '%s'", name), e2);
        }
    }
}
