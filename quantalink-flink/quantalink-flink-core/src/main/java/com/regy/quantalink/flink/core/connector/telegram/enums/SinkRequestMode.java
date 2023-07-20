package com.regy.quantalink.flink.core.connector.telegram.enums;

/**
 * @author regy
 */

public enum SinkRequestMode {
    SINGLE("single"), BATCH("batch");

    private String mode;

    SinkRequestMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
