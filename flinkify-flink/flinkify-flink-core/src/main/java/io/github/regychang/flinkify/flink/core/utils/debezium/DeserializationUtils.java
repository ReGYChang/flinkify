package io.github.regychang.flinkify.flink.core.utils.debezium;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.github.regychang.flinkify.flink.core.connector.serialization.DeserializationAdapter;
import java.util.Optional;

public class DeserializationUtils {

    /**
     * Extracts the DebeziumDeserializationSchema from a DeserializationAdapter.
     *
     * @param <T> The type parameter for the deserialization schema
     * @param adapter The DeserializationAdapter to extract the schema from
     * @return The DebeziumDeserializationSchema
     * @throws UnsupportedOperationException if the adapter is null, not a
     *     DebeziumDeserializationAdapter, or doesn't contain a valid schema
     */
    @SuppressWarnings("unchecked")
    public static <T> DebeziumDeserializationSchema<T> extractDeserializationSchema(
            DeserializationAdapter<T, ?> adapter) {
        if (adapter == null) {
            throw new UnsupportedOperationException("DeserializationAdapter is null");
        }

        if (!(adapter instanceof DebeziumDeserializationAdapter)) {
            throw new UnsupportedOperationException(
                    "Adapter is not a DebeziumDeserializationAdapter");
        }

        DebeziumDeserializationAdapter<T> debeziumAdapter =
                (DebeziumDeserializationAdapter<T>) adapter;

        return Optional.ofNullable(debeziumAdapter.getDeserializationSchema())
                .orElseThrow(
                        () ->
                                new UnsupportedOperationException(
                                        "No valid deserialization schema found in DebeziumDeserializationAdapter"));
    }
}
