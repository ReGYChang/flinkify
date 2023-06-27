package com.regy.quantalink.common.utils;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author regy
 */
public class KeyUtils {
    public static Integer[] createRebalancedKeys(int parallelism) {
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        int maxRandomKey = parallelism * 12;
        Map<Integer, Integer> keySubIndexMap = new HashMap<>(256);
        for (int randomKey = 0; randomKey < maxRandomKey; randomKey++) {
            int subtaskIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(randomKey, maxParallelism, parallelism);
            if (keySubIndexMap.containsKey(subtaskIndex)) {
                continue;
            }
            keySubIndexMap.put(subtaskIndex, randomKey);
        }
        return keySubIndexMap.values().toArray(new Integer[0]);
    }

    public static Integer[] createRebalancedKeys(int parallelism, int hash) {
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        Map<Integer, Integer> keySubIndexMap = new HashMap<>(256);
        int base = 0;
        while (keySubIndexMap.size() < parallelism) {
            int subtaskIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(hash + base, maxParallelism, parallelism);
            keySubIndexMap.put(subtaskIndex, base);
            base++;
        }
        return keySubIndexMap.values().toArray(new Integer[0]);
    }

    public static Integer[] createRebalancedKeys(int parallelism, int hash, Random random) {
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        Map<Integer, Integer> keySubIndexMap = new HashMap<>(256);
        while (keySubIndexMap.size() < parallelism) {
            int randomKey = random.nextInt(Integer.MAX_VALUE);
            int subtaskIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(hash + randomKey, maxParallelism, parallelism);
            keySubIndexMap.put(subtaskIndex, randomKey);
        }
        return keySubIndexMap.values().toArray(new Integer[0]);
    }

}
