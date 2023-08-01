package com.regy.quantalink.flink.core.connector.telegram.sink;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.flink.core.connector.SinkConnector;
import com.regy.quantalink.flink.core.connector.telegram.config.TelegramOptions;
import com.regy.quantalink.flink.core.connector.telegram.func.SetupPayloadFunc;
import com.regy.quantalink.flink.core.connector.telegram.serialization.DefaultTelegramHttpConvert;
import com.regy.quantalink.flink.core.connector.telegram.serialization.TelegramSerializationAdapter;

import com.alibaba.fastjson2.JSON;
import com.getindata.connectors.http.HttpSink;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author regy
 */
public class TelegramSinkConnector<T> extends SinkConnector<T, TelegramPayload> {
    private final String endpointUrl;
    private final Properties httpProperties = new Properties();
    private final String httpMethod;
    private final String parseMode;
    private final String chatId;

    public TelegramSinkConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);

        String token = config.getNotNull(TelegramOptions.TOKEN, "Telegram sink connector token must not be null");
        String telegramMethod = config.getNotNull(TelegramOptions.BOT_METHOD, "Telegram sink connector token must not be null").getMethod();
        this.chatId = config.getNotNull(TelegramOptions.CHAT_ID, "Telegram sink connector chat-id must not be null");
        this.httpMethod = config.get(TelegramOptions.HTTP_METHOD).name();
        this.parseMode = config.get(TelegramOptions.PARSE_MODE).getMode();
        this.endpointUrl = String.format("https://api.telegram.org/bot%s/%s", token, telegramMethod);
        this.httpProperties.putAll(
                config.get(TelegramOptions.HEADER).toMap().entrySet()
                        .stream().collect(Collectors.toMap(e -> "gid.connector.http.sink.header." + e.getKey(), Map.Entry::getValue)));

        if (config.contains(TelegramOptions.ALLOW_SELF_SIGNED)) {
            httpProperties.put("gid.connector.http.security.cert.server.allowSelfSigned", config.get(TelegramOptions.ALLOW_SELF_SIGNED).toString());
        }
        if (config.contains(TelegramOptions.SINK_REQUEST_MODE)) {
            httpProperties.put("gid.connector.http.sink.writer.request.mode", config.get(TelegramOptions.SINK_REQUEST_MODE).getMode());
        }
    }

    @Override
    public DataStreamSink<TelegramPayload> createSinkDataStream(DataStream<TelegramPayload> stream) {

        SingleOutputStreamOperator<TelegramPayload> payloadStream =
                stream.map(new SetupPayloadFunc(parseMode, chatId)).returns(Types.POJO(TelegramPayload.class));
        TelegramSerializationAdapter serializationAdapter =
                Optional.ofNullable((TelegramSerializationAdapter) getSerializationAdapter())
                        .orElse(new TelegramSerializationAdapter(JSON::toJSONBytes));

        var httpSink = HttpSink.<TelegramPayload>builder()
                .setEndpointUrl(endpointUrl)
                .setElementConverter(new DefaultTelegramHttpConvert(httpMethod, serializationAdapter.getSerializationSchema()))
                .setProperties(httpProperties)
                .build();

        return payloadStream.sinkTo(httpSink);
    }
}
