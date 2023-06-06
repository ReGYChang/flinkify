package com.regy.quantalink.common.config;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.Preconditions;

/**
 * @author regy
 */
public class ConfigOption<T> {
    public static final Description EMPTY_DESCRIPTION = Description.builder().text("").build();
    private final String key;
    private final T defaultValue;
    private final Description description;
    private final Class<?> clazz;
    private final boolean isList;

    public Class<?> getClazz() {
        return this.clazz;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConfigOption<?> that = (ConfigOption<?>) o;

        return new EqualsBuilder().append(isList, that.isList).append(key, that.key).append(defaultValue, that.defaultValue).append(description, that.description).append(clazz, that.clazz).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(key).append(defaultValue).append(description).append(clazz).append(isList).toHashCode();
    }

    @Override
    public String toString() {
        return String.format("Key: '%s' , default: %s", this.key, this.defaultValue);
    }
}

