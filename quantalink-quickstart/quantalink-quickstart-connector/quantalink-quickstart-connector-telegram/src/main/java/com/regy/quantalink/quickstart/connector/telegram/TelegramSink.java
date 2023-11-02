package com.regy.quantalink.quickstart.connector.telegram;

import com.regy.quantalink.common.exception.FlinkException;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.telegram.serialization.TelegramSerializationAdapter;
import com.regy.quantalink.flink.core.connector.telegram.sink.TelegramPayload;
import com.regy.quantalink.flink.core.streaming.FlinkDataStream;
import com.regy.quantalink.flink.core.streaming.FlinkStreaming;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingContext;
import com.regy.quantalink.flink.core.streaming.FlinkStreamingInitializer;
import com.regy.quantalink.quickstart.connector.telegram.entity.Transaction;
import com.regy.quantalink.quickstart.connector.telegram.function.ApiBatchFunction;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


/**
 * @author regy
 */
public class TelegramSink extends FlinkStreaming {

    public static void main(String[] args) throws Exception {
        FlinkStreamingInitializer initializer = new FlinkStreamingInitializer.Builder()
                .withExecutionEnvironmentSetup(
                        env -> {
                            env.enableCheckpointing(3000);
                            env.setParallelism(1);
                        })

                .withSinkConnectorSetup(
                        (sinkConnector, config) -> {
                            sinkConnector.setSerializationAdapter(new TelegramSerializationAdapter(JSON::toJSONBytes));
                            sinkConnector.setTransformFunc(
                                    (input, collector) -> {
                                        if (input.amountIn > 0) {
                                            collector.collect(new TelegramPayload(input.toMarkdownString()));
                                        }
                                    });
                        },
                        TypeInformation.get(Transaction.class), TypeInformation.get(TelegramPayload.class)).build();

        (new TelegramSink()).run(args, initializer);
    }

    @Override
    protected void execute(FlinkStreamingContext ctx) throws FlinkException {
        DataStreamSource<Transaction> stream = ctx.getEnv().addSource(new ApiBatchFunction());
        FlinkDataStream.ofDataStream(stream, ctx).sink(TypeInformation.get(TelegramPayload.class));
    }
}
