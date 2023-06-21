package com.regy.quantalink.common.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * @author regy
 */
public class TimeUtils {

    public static long toMillis(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long toMillis(Duration duration) {
        return duration.toMillis();
    }

    public static LocalDateTime toDateTime(long millis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    public static LocalDateTime toDateTime(Duration duration) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(toMillis(duration)), ZoneOffset.UTC);
    }

    public static Date toDate(LocalDateTime dateTime) {
        return Date.from(dateTime.toInstant(ZoneOffset.UTC));
    }

    public static Duration toDuration(long millis) {
        return Duration.ofMillis(millis);
    }

    public static Duration toDuration(LocalDateTime dateTime) {
        return Duration.ofMillis(toMillis(dateTime));
    }

    public static boolean isEpochSecond(long value) {
        return value < 100_000_000_000L;
    }

    private TimeUtils() {
    }
}
