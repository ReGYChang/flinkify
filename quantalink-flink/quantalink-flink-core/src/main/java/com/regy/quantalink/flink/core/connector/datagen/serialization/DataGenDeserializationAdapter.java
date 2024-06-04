package com.regy.quantalink.flink.core.connector.datagen.serialization;

import com.regy.quantalink.common.config.Configuration;
import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.config.SourceConnectorOptions;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

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
