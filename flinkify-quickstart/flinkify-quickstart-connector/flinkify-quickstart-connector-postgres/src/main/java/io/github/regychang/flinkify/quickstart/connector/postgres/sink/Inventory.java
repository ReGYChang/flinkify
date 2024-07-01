package io.github.regychang.flinkify.quickstart.connector.postgres.sink;

import io.github.regychang.flinkify.flink.core.utils.jdbc.Column;
import lombok.Data;

import java.sql.JDBCType;

@Data
public class Inventory {

    @Column(
            name = "name",
            type = JDBCType.VARCHAR)
    private String name;

    @Column(
            name = "description",
            type = JDBCType.VARCHAR)
    private String description;

    @Column(
            name = "quantity",
            type = JDBCType.INTEGER)
    private Integer quantity;

    @Column(
            name = "price",
            type = JDBCType.DOUBLE)
    private Double price;
}