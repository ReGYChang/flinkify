package io.github.regychang.flinkify.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CopyUtils.class);

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> deepCopy(Map<K, V> original) {

        if (original == null) {
            return null;
        }

        Map<K, V> copy = new HashMap<>();
        for (Map.Entry<K, V> entry : original.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            V valueCopy;

            if (value instanceof Map) {
                valueCopy = (V) deepCopy((Map<?, ?>) value);
            } else if (value instanceof List) {
                valueCopy = (V) deepCopy((List<?>) value);
            } else if (value != null) {
                valueCopy = (V) deepCopy(value);
            } else {
                valueCopy = value;
            }

            copy.put(key, valueCopy);
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> deepCopy(List<T> original) {

        if (original == null) {
            return null;
        }

        List<T> copy = new ArrayList<>(original.size());
        for (T element : original) {
            T elementCopy;
            if (element instanceof Map) {
                elementCopy = (T) deepCopy((Map<?, ?>) element);
            } else if (element instanceof List) {
                elementCopy = (T) deepCopy((List<?>) element);
            } else if (element != null) {
                elementCopy = (T) deepCopy(element);
            } else {
                elementCopy = null;
            }
            copy.add(elementCopy);
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    public static <T> T deepCopy(T original) {
        T copy = null;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(original);
            objectOutputStream.close();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            copy = (T) objectInputStream.readObject();
            objectInputStream.close();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Failed to copy object: {}", original, e);
        }
        return copy;
    }
}
