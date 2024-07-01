package io.github.regychang.flinkify.flink.core.utils.jdbc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnUtils {

    public static <T> List<ColumnInfo> getColumnInfo(Class<T> recordClass) {
        return Arrays.stream(recordClass.getDeclaredFields())
                .filter(
                        field ->
                                field.isAnnotationPresent(Column.class))
                .map(
                        field -> {
                            Column column = field.getAnnotation(Column.class);
                            return new ColumnInfo(column.name(), column.type(), field);
                        })
                .collect(Collectors.toList());
    }
}
