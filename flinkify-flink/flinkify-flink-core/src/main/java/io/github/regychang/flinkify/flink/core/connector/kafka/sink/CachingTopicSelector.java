package io.github.regychang.flinkify.flink.core.connector.kafka.sink;

import org.apache.flink.connector.kafka.sink.TopicSelector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class CachingTopicSelector<IN> implements Function<IN, String>, Serializable {

    private static final int CACHE_RESET_SIZE = 5;

    private final Map<IN, String> cache;

    private final TopicSelector<IN> topicSelector;

    public CachingTopicSelector(TopicSelector<IN> topicSelector) {
        this.topicSelector = topicSelector;
        this.cache = new HashMap<>();
    }

    public String apply(IN in) {
        String topic = this.cache.getOrDefault(in, this.topicSelector.apply(in));
        this.cache.put(in, topic);
        if (this.cache.size() == CACHE_RESET_SIZE) {
            this.cache.clear();
        }

        return topic;
    }
}
