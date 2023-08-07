package com.regy.quantalink.flink.core.connector.csv.serialization;

import com.regy.quantalink.common.type.TypeInformation;
import com.regy.quantalink.flink.core.connector.serialization.DeserializationAdapter;

import org.apache.flink.formats.csv.CsvReaderFormat;

/**
 * @author regy
 */
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
