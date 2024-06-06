package io.github.regychang.flinkify.flink.core.connector.csv.serialization;

import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.formats.csv.CsvReaderFormat;

public class CsvDeserializationAdapter<T> extends DeserializationAdapter<T, CsvReaderFormat<T>> {

    private final CsvReaderFormat<T> csvReaderFormat;

    public CsvDeserializationAdapter(CsvReaderFormat<T> csvReaderFormat) {
        super(TypeInformation.get(csvReaderFormat.getProducedType()));
        this.csvReaderFormat = csvReaderFormat;
    }

    @Override
    public CsvReaderFormat<T> getDeserializationSchema() {
        return csvReaderFormat;
    }
}
