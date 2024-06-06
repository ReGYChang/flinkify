package io.github.regychang.flinkify.flink.core.connector.telegram.enums;

import lombok.Getter;

@Getter
public enum ParseMode {

    TEXT(""),

    MARKDOWN("Markdown"),

    MARKDOWN_V2("MarkdownV2"),

    HTML("HTML");

    private final String mode;

    ParseMode(String mode) {
        this.mode = mode;
    }
}
