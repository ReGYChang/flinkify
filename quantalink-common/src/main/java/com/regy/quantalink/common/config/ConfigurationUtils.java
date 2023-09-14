package com.regy.quantalink.common.config;

import com.regy.quantalink.common.exception.ConfigurationException;
import com.regy.quantalink.common.exception.ErrCode;
import com.regy.quantalink.common.type.TypeInformation;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author regy
 */
public final class ConfigurationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (Character.class.equals(clazz)) {
            return (T) convertToCharacter(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (clazz == Duration.class) {
            return (T) convertToDuration(rawValue);
        } else if (clazz == Map.class) {
            return (T) convertToMap(rawValue);
        } else if (clazz == Configuration.class) {
            return (T) convertToConfiguration(rawValue);
        } else if (clazz.isInterface()) {
            return (T) convertToInterface(rawValue, (Class<? extends Class<?>>) clazz);
        } else if (TypeInformation.class.equals(clazz)) {
            return (T) convertToTypeInformation(rawValue);
        } else if (clazz == DataType.class) {
            return (T) convertToDataType(rawValue);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertToList(Object rawValue, Class<?> atomicClass) {
        try {
            return (T) ((List<?>) rawValue).stream().map(
                    s -> convertValue(s, atomicClass)).collect(Collectors.toList());
        } catch (ClassCastException e) {
            throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, "Failed to parse field into list, please check your configuration format");
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> convertToMap(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<? extends T> convertToInterface(Object o, Class<T> clazz) {
        try {
            Class<?> oClazz = Class.forName(o.toString());
            if (clazz.isAssignableFrom(oClazz)) {
                return (Class<? extends T>) oClazz;
            } else {
                throw new IllegalArgumentException(String.format("Class '%s' is not assignable from '%s'", clazz, o));
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, String.format("Class '%s' has not found, please check the the class file has been loaded correctly", o));
        }
    }

    public static TypeInformation<?> convertToTypeInformation(Object o) {
        if (o instanceof TypeInformation) {
            return (TypeInformation<?>) o;
        }

        try {
            String[] typeNames = o.toString().split(",");
            if (typeNames.length < 1) {
                throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, "Invalid typeInformation representation, rawType must not be null");
            }

            Class<?> rawType = Class.forName(typeNames[0].trim());
            Type[] actualTypeArguments = new Type[typeNames.length - 1];
            for (int i = 1; i < typeNames.length; i++) {
                actualTypeArguments[i - 1] = Class.forName(typeNames[i].trim());
            }

            return TypeInformation.get(rawType, actualTypeArguments);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, String.format("Class not found, please check the class file has been loaded correctly: %s", e.getMessage()));
        }
    }

    public static DataType convertToDataType(Object o) {
        if (o instanceof DataType) {
            return (DataType) o;
        }

        Pattern pattern = Pattern.compile("([a-zA-Z0-9]+)(?:-(\\d+)(?:,\\s*(\\d+))?)?");
        Matcher matcher = pattern.matcher(o.toString());

        if (matcher.matches()) {
            String type = matcher.group(1).toUpperCase();
            String firstParam = matcher.group(2);
            String secondParam = matcher.group(3);

            switch (type) {
                case "CHAR":
                    return DataTypes.CHAR(Integer.parseInt(firstParam));
                case "VARCHAR":
                    return DataTypes.VARCHAR(Integer.parseInt(firstParam));
                case "BINARY":
                    return DataTypes.BINARY(Integer.parseInt(firstParam));
                case "VARBINARY":
                    return DataTypes.VARBINARY(Integer.parseInt(firstParam));
                case "DECIMAL":
                    if (firstParam == null || secondParam == null) {
                        return DataTypes.DECIMAL(2, 2);
                    } else {
                        return DataTypes.DECIMAL(Integer.parseInt(firstParam), Integer.parseInt(secondParam));
                    }
                case "DATETIME":
                case "DATETIMEV2":
                    return DataTypes.TIMESTAMP(6);
                case "STRING":
                    return DataTypes.STRING();
                case "BOOLEAN":
                    return DataTypes.BOOLEAN();
                case "BYTES":
                    return DataTypes.BYTES();
                case "TINYINT":
                    return DataTypes.TINYINT();
                case "SMALLINT":
                    return DataTypes.SMALLINT();
                case "INT":
                    return DataTypes.INT();
                case "BIGINT":
                    return DataTypes.BIGINT();
                case "FLOAT":
                    return DataTypes.FLOAT();
                case "DOUBLE":
                    return DataTypes.DOUBLE();
                case "DATE":
                case "DATEV2":
                    return DataTypes.DATE();
                default:
                    throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, "Invalid data type string: " + o);
            }
        }

        throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, "Could not parse value for data type: " + o);
    }

    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new ConfigurationException(
                                        ErrCode.PARSING_CONFIG_FAILED,
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    public static Duration convertToDuration(Object o) {
        if (o.getClass() == Duration.class) {
            return (Duration) o;
        }

        return TimeUtils.parseDuration(o.toString());
    }

    public static Character convertToCharacter(Object o) {
        if (o instanceof Character) {
            return (Character) o;
        } else if (o instanceof String && ((String) o).length() == 1) {
            return ((String) o).charAt(0);
        } else {
            throw new ConfigurationException(
                    ErrCode.PARSING_CONFIG_FAILED,
                    String.format("Invalid character value input '%s' in the configuration", o));
        }
    }

    public static String convertToString(Object o) {
        return o.toString();
    }

    public static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new ConfigurationException(
                        ErrCode.PARSING_CONFIG_FAILED,
                        String.format(
                                "Configuration value '%s' overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    public static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    public static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    public static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new ConfigurationException(
                        ErrCode.PARSING_CONFIG_FAILED,
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    public static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    public static Configuration loadYamlConfigFromPath(String path) {
        try (FileInputStream stream = new FileInputStream(path)) {
            return loadYamlConfigFromStream(stream);
        } catch (IOException e) {
            LOG.warn("Error reading YAML configuration file from path.", e);
            return null;
        }
    }

    public static Configuration loadYamlConfigFromClasspath(ClassLoader classLoader, String name) {
        try (InputStream stream = Objects.requireNonNull(classLoader.getResourceAsStream(name))) {
            return ConfigurationUtils.loadYamlConfigFromStream(stream);
        } catch (IOException | NullPointerException e) {
            LOG.warn("Configuration files not found, initial flink job with empty configuration", e);
            return new Configuration();
        }
    }

    public static Configuration loadYamlConfigFromStream(InputStream stream) {
        Yaml yaml = new Yaml();
        Map<String, Object> configMap = yaml.load(stream);
        return Configuration.fromMap(configMap);
    }

    public static Configuration convertToConfiguration(Object o) {
        if (o instanceof Map) {
            return Configuration.fromMap(convertToMap(o));
        } else if (o instanceof Configuration) {
            return (Configuration) o;
        } else {
            throw new ConfigurationException(ErrCode.PARSING_CONFIG_FAILED, String.format("Could not parse value for configuration: %s.", o));
        }
    }

    private ConfigurationUtils() {
    }
}

