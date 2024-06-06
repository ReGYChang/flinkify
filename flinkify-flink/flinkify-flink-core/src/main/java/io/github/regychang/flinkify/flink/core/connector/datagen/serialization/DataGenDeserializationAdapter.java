package io.github.regychang.flinkify.flink.core.connector.datagen.serialization;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.config.SourceConnectorOptions;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;

import io.github.regychang.java.faker.Options;


public class DataGenDeserializationAdapter<T> extends DeserializationAdapter<T, Options> {

    private final Options options;

    public DataGenDeserializationAdapter(Configuration config) {
        this(config, new Options());
    }

    @SuppressWarnings("unchecked")
    public DataGenDeserializationAdapter(Configuration config, Options options) {
        super((TypeInformation<T>) config.getNotNull(SourceConnectorOptions.DATA_TYPE));
        this.options = options;
    }

    @Override
    public Options getDeserializationSchema() {
        return options;
    }
}
