package com.regy.quantalink.common.config;

import com.regy.quantalink.common.type.TypeInformation;

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * @author regy
 */
public class ConfigOptions {

    public static OptionBuilder key(String key) {
        Preconditions.checkNotNull(key);
        return new OptionBuilder(key);
    }

    public static class ListConfigOptionBuilder<E> {
        private final String key;
        private final Class<E> clazz;

        ListConfigOptionBuilder(String key, Class<E> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        @SafeVarargs
        public final ConfigOption<List<E>> defaultValues(E... values) {
            return new ConfigOption<>(this.key, this.clazz, ConfigOption.EMPTY_DESCRIPTION, Arrays.asList(values), true);
        }

        public ConfigOption<List<E>> noDefaultValue() {
            return new ConfigOption<>(this.key, this.clazz, ConfigOption.EMPTY_DESCRIPTION, null, true);
        }
    }

    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypedConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        public ListConfigOptionBuilder<T> asList() {
            return new ListConfigOptionBuilder<>(this.key, this.clazz);
        }

        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(this.key, this.clazz, ConfigOption.EMPTY_DESCRIPTION, value, false);
        }

        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(this.key, this.clazz, Description.builder().text("").build(), null, false);
        }
    }

    public static final class OptionBuilder {
        private final String key;

        OptionBuilder(String key) {
            this.key = key;
        }

        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(this.key, Boolean.class);
        }

        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(this.key, Integer.class);
        }

        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(this.key, Long.class);
        }

        public TypedConfigOptionBuilder<Float> floatType() {
            return new TypedConfigOptionBuilder<>(this.key, Float.class);
        }

        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(this.key, Double.class);
        }

        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(this.key, String.class);
        }

        public TypedConfigOptionBuilder<Duration> durationType() {
            return new TypedConfigOptionBuilder<>(this.key, Duration.class);
        }

        @SuppressWarnings("unchecked")
        public TypedConfigOptionBuilder<TypeInformation<?>> type() {
            return new TypedConfigOptionBuilder<>(this.key, (Class<TypeInformation<?>>) (Class<?>) TypeInformation.class);
        }

        public TypedConfigOptionBuilder<DataType> dataType() {
            return new TypedConfigOptionBuilder<>(this.key, DataType.class);
        }

        public <T extends Enum<T>> TypedConfigOptionBuilder<T> enumType(Class<T> enumClass) {
            return new TypedConfigOptionBuilder<>(this.key, enumClass);
        }

        public TypedConfigOptionBuilder<Configuration> configType() {
            return new TypedConfigOptionBuilder<>(this.key, Configuration.class);
        }
    }

    private ConfigOptions() {
    }
}

