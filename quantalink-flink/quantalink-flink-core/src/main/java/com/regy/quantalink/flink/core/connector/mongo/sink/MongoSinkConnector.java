package com.regy.quantalink.flink.core.connector.mongo.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.mongo.config.MongoOptions;
import com.regy.quantalink.flink.core.connector.mongo.serialization.BsonDocumentParser;
import com.regy.quantalink.flink.core.connector.mongo.serialization.MongoSerializationAdapter;

import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * @author regy
 */
public class MongoSinkConnector<T> extends SinkConnector<T, T> {

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
    public DataStreamSink<T> createSinkDataStream(DataStream<T> stream) {
        try {
            MongoSerializationAdapter<T> serializationAdapter =
                    Optional.ofNullable((MongoSerializationAdapter<T>) getSerializationAdapter())
                            .orElse(new MongoSerializationAdapter<>(
                                    (input, cxt) -> {
                                        try {
                                            return new InsertOneModel<>(BsonDocumentParser.parse(input));
                                        } catch (IllegalAccessException e) {
                                            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED, String.format("Failed to parse value '%s' to bson document for mongodb sink connector", input));
                                        }
                                    }, getOutputType()));
            MongoSink<T> sink = MongoSink.<T>builder()
                    .setUri(uri)
                    .setDatabase(database)
                    .setCollection(collection)
                    .setBatchSize(batchSize)
                    .setBatchIntervalMs(batchIntervalMs)
                    .setMaxRetries(maxRetries)
                    .setSerializationSchema(serializationAdapter.getSerializationSchema())
                    .build();
            return stream.sinkTo(sink);
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("MongoDB sink connector '%s' serialization adapter must be '%s', could not assign other serialization adapter", getName(), MongoSerializationAdapter.class), e);
        } catch (Exception e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("Failed to initialize MongoDB sink connector '%s'", getName()), e);
        }
    }
}
