package com.regy.quantalink.flink.core.connector.telegram.enums;

/**
 * @author regy
 */

public enum ParseMode {
    TEXT(""), MARKDOWN("Markdown"), MARKDOWN_V2("MarkdownV2"), HTML("HTML");

    private String mode;

    ParseMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
