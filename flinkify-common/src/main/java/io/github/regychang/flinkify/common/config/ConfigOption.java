package io.github.regychang.flinkify.common.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.Preconditions;

@ToString
@EqualsAndHashCode
public class ConfigOption<T> {

    public static final Description EMPTY_DESCRIPTION = Description.builder().text("").build();

    @Getter
    private final Class<?> clazz;

    @Getter
    private final String key;

    private final T defaultValue;

    private final Description description;

    private final boolean isList;

    public boolean isList() {
        return this.isList;
    }

    ConfigOption(String key, Class<?> clazz, Description description, T defaultValue, boolean isList) {
        this.key = Preconditions.checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.clazz = Preconditions.checkNotNull(clazz);
        this.isList = isList;
    }

    public ConfigOption<T> withDescription(String description) {
        return this.withDescription(Description.builder().text(description).build());
    }

    public ConfigOption<T> withDescription(Description description) {
        return new ConfigOption<>(this.key, this.clazz, description, this.defaultValue, this.isList);
    }

    public String key() {
        return this.key;
    }

    public boolean hasDefaultValue() {
        return this.defaultValue != null;
    }

    public T defaultValue() {
        return this.defaultValue;
    }

    public Description description() {
        return this.description;
    }
}
