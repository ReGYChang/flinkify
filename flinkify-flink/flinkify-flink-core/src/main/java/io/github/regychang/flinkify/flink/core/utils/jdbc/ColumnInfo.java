package io.github.regychang.flinkify.flink.core.utils.jdbc;

import lombok.Data;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.JDBCType;

@Data
public class ColumnInfo implements Serializable {

    private final String name;

    private final JDBCType type;

    private final Field field;
}
