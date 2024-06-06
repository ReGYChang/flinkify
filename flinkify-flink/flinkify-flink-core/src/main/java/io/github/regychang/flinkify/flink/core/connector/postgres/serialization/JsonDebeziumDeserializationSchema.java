package io.github.regychang.flinkify.flink.core.connector.postgres.serialization;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<SourceRecord> {

    private static final long serialVersionUID = 1L;

    public void deserialize(SourceRecord record, Collector<SourceRecord> out) {
        out.collect(record);
    }

    public TypeInformation<SourceRecord> getProducedType() {
        return Types.GENERIC(SourceRecord.class);
    }
}
