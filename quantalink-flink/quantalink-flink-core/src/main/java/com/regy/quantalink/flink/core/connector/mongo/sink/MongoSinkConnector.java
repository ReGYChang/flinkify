package com.regy.quantalink.flink.core.connector.mongo.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.mongo.config.MongoOptions;

import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

    @Override
    public DataStreamSink<T> getSinkDataStream(DataStream<T> stream) {
        MongoSink<T> sink = MongoSink.<T>builder()
                .setUri(uri)
                .setDatabase(database)
                .setCollection(collection)
                .setBatchSize(batchSize)
                .setBatchIntervalMs(batchIntervalMs)
                .setMaxRetries(maxRetries)
                .setSerializationSchema(super.serializationAdapter.getSerializationSchema())
                .build();
        return stream.sinkTo(sink).name(super.connectorName).setParallelism(super.parallelism).disableChaining();
    }
}
