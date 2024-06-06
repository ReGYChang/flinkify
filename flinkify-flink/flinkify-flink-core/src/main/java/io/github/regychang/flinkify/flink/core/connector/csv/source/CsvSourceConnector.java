package io.github.regychang.flinkify.flink.core.connector.csv.source;

import io.github.regychang.flinkify.common.config.Configuration;
import io.github.regychang.flinkify.common.exception.ErrCode;
import io.github.regychang.flinkify.common.exception.FlinkException;
import io.github.regychang.flinkify.common.type.TypeInformation;
import io.github.regychang.flinkify.flink.core.connector.SourceConnector;
import io.github.regychang.flinkify.flink.core.connector.csv.config.CsvOptions;
import io.github.regychang.flinkify.flink.core.connector.csv.serialization.CsvDeserializationAdapter;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.util.Optional;


public class CsvSourceConnector<T> extends SourceConnector<T> {

    private final String filePath;

    private final boolean usesHeader;

    private final boolean reorderColumns;

    private final boolean strictHeaders;

    private final boolean skipFirstDataRow;

    private final boolean allowComments;

    private final Character columnSeparator;

    private final String arrayElementSeparator;

    private final Character quoteChar;

    private final Character escapeChar;

    private final String lineSeparator;

    private final String nullValue;

    private final boolean ignoreParseErrors;

    private final CsvReaderFormat<T> format;

    public CsvSourceConnector(StreamExecutionEnvironment env, Configuration config) {
        super(env, config);
        this.filePath = config.getNotNull(CsvOptions.FILE_PATH, "CSV source connector file-path must not be null");
        this.usesHeader = config.get(CsvOptions.USES_HEADER);
        this.reorderColumns = config.get(CsvOptions.REORDER_COLUMNS);
        this.strictHeaders = config.get(CsvOptions.STRICT_HEADERS);
        this.skipFirstDataRow = config.get(CsvOptions.SKIP_FIRST_DATA_ROW);
        this.allowComments = config.get(CsvOptions.ALLOW_COMMENTS);
        this.columnSeparator = config.get(CsvOptions.COLUMN_SEPARATOR);
        this.arrayElementSeparator = config.get(CsvOptions.ARRAY_ELEMENT_SEPARATOR);
        this.quoteChar = config.get(CsvOptions.QUOTE_CHAR);
        this.escapeChar = config.get(CsvOptions.ESCAPE_CHAR);
        this.lineSeparator = config.get(CsvOptions.LINE_SEPARATOR);
        this.nullValue = config.get(CsvOptions.NULL_VALUE);
        this.ignoreParseErrors = config.get(CsvOptions.IGNORE_PARSE_ERRORS);
        this.format = createCsvReaderFormat();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataStreamSource<T> getSourceDataStream() throws FlinkException {
        try {
            CsvDeserializationAdapter<T> deserializer =
                    Optional.ofNullable((CsvDeserializationAdapter<T>) getDeserializationAdapter())
                            .orElse(new CsvDeserializationAdapter<>(format));
            FileSource<T> source = FileSource.forRecordStreamFormat(deserializer.getDeserializationSchema(), new Path(filePath)).build();

            return getEnv().fromSource(source, WatermarkStrategy.noWatermarks(), getName());
        } catch (ClassCastException e) {
            throw new FlinkException(ErrCode.STREAMING_CONNECTOR_FAILED,
                    String.format("CSV source connector '%s' deserialization adapter must be '%s', could not assign other deserialization adapter",
                            getName(), CsvDeserializationAdapter.class), e);
        }
    }

    private CsvReaderFormat<T> createCsvReaderFormat() {
        CsvReaderFormat<T> csvReaderFormat = CsvReaderFormat.forSchema(JacksonMapperFactory::createCsvMapper,
                (mapper) -> {
                    CsvSchema schema = mapper.schemaFor(getTypeInfo().getRawType());
                    if (usesHeader) {
                        schema.usesHeader();
                    }
                    if (reorderColumns) {
                        schema.reordersColumns();
                    }
                    if (strictHeaders) {
                        schema.strictHeaders();
                    }
                    if (skipFirstDataRow) {
                        schema.skipsFirstDataRow();
                    }
                    if (allowComments) {
                        schema.allowsComments();
                    }
                    if (columnSeparator != null) {
                        schema.withColumnSeparator(columnSeparator);
                    }
                    if (arrayElementSeparator != null) {
                        schema.withArrayElementSeparator(arrayElementSeparator);
                    }
                    if (quoteChar != null) {
                        schema.withQuoteChar(quoteChar);
                    }
                    if (escapeChar != null) {
                        schema.withEscapeChar(escapeChar);
                    }
                    if (lineSeparator != null) {
                        schema.withLineSeparator(lineSeparator);
                    }
                    if (nullValue != null) {
                        schema.withNullValue(nullValue);
                    }
                    return schema;
                }, TypeInformation.convertToFlinkType(getTypeInfo()));
        if (ignoreParseErrors) {
            return csvReaderFormat.withIgnoreParseErrors();
        }
        return csvReaderFormat;
    }
}
