package com.regy.quantalink.common.config;

import com.regy.quantalink.common.exception.ConfigurationException;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.utils.CopyUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author regy
 */
public class Configuration implements Serializable {
    private final Map<String, Object> confData;

    public Configuration() {
        this.confData = new HashMap<>();
    }

    public Configuration(Configuration other) {
        this.confData = new HashMap<>(other.confData);
    }

    public static Configuration fromMap(Map<String, Object> map) {
        final Configuration configuration = new Configuration();
        map.forEach(configuration::setValueInternal);
        return configuration;
    }

    public <T> T get(ConfigOption<T> configOption) {
        return getValueFromOption(configOption).orElse(configOption.defaultValue());
    }

    public <T> T get(ConfigOption<T> configOption, T overrideDefault) {
        return getValueFromOption(configOption).orElse(overrideDefault);
    }

    public <T> T getNotNull(ConfigOption<T> configOption, String message) {
        return Optional.ofNullable(this.get(configOption)).orElseThrow(() -> new ConfigurationException(ErrCode.MISSING_CONFIG_FIELD, message));
    }

    @SuppressWarnings("unchecked")
    public <T> void set(ConfigOption<T> option, T value) {
        String[] parentPrefix = option.key().split("\\.");
        Optional<Object> parent = getValueFromPrefix(Arrays.copyOfRange(parentPrefix, 0, parentPrefix.length), this.confData, null);
        Object parentObj = parent.orElseThrow(() -> new ConfigurationException(ErrCode.MISSING_CONFIG_FIELD, String.format("Could not set value '%s' for key '%s'", value, option.key())));
        if (parentObj instanceof Map) {
            ((Map<String, Object>) parentObj).put(parentPrefix[parentPrefix.length - 1], value);
        } else {
            throw new ConfigurationException(ErrCode.MISSING_CONFIG_FIELD, String.format("Could not set value '%s' for key '%s' because the parent field '%s' is not a map", value, option.key(), parentPrefix[parentPrefix.length - 1]));
        }
    }

    public boolean remove(ConfigOption<?> configOption) {
        synchronized (this.confData) {
            return this.confData.remove(configOption.key()) != null;
        }
    }

    private <T> Optional<T> getValueFromOption(ConfigOption<T> option) {
        String[] prefixKeys = option.key().split("\\.");
        Class<?> clazz = option.getClazz();

        Optional<Object> value;
        if (prefixKeys.length > 1) {
            value = getValueFromPrefix(prefixKeys, this.confData, null);
        } else {
            value = Optional.ofNullable(this.confData.get(option.key()));
        }

        try {
            return option.isList() ?
                    value.map(v -> ConfigurationUtils.convertToList(v, clazz)) :
                    value.map(v -> ConfigurationUtils.convertValue(v, clazz));
        } catch (ConfigurationException e) {
            throw new ConfigurationException(
                    ErrCode.PARSING_CONFIG_FAILED,
                    String.format("Could not parse value '%s' for key '%s'.", value.map(Object::toString).orElse(""), option.key()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Optional<Object> getValueFromPrefix(String[] prefixKeys, Map<String, Object> configMap, Object value) {
        if (prefixKeys.length < 1) {
            return Optional.ofNullable(value);
        }

        Object newValue = configMap.get(prefixKeys[0]);
        if (newValue instanceof Map) {
            return getValueFromPrefix(Arrays.copyOfRange(prefixKeys, 1, prefixKeys.length), (Map<String, Object>) newValue, newValue);
        }
        return Optional.ofNullable(newValue);
    }

    private <T> void setValueInternal(String key, T value) {
        if (key == null) {
            throw new ConfigurationException(ErrCode.MISSING_CONFIG_FIELD, "Configuration key must not be null");
        }
        if (value == null) {
            throw new ConfigurationException(ErrCode.MISSING_CONFIG_FIELD, "Configuration value must not be null");
        }

        synchronized (this.confData) {
            this.confData.put(key, value);
        }
    }

    public boolean contains(ConfigOption<?> option) {
        return getValueFromOption(option).isPresent();
    }

    public Map<String, Object> toMap() {
        synchronized (this.confData) {
            return CopyUtils.deepCopy(this.confData);
        }
    }
}

