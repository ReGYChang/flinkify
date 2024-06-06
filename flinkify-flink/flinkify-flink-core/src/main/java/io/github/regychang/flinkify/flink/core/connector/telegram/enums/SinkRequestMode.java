package io.github.regychang.flinkify.flink.core.connector.telegram.enums;

import lombok.Getter;

@Getter
public enum SinkRequestMode {

    SINGLE("single"),

    BATCH("batch");

    private final String mode;

    SinkRequestMode(String mode) {
        this.mode = mode;
    }
}
