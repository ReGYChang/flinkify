package io.github.regychang.flinkify.common.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

public class TimeUtils {

    public static long toMillis(LocalDateTime dateTime) {
        return toMillis(dateTime, ZoneOffset.UTC);
    }

    public static long toMillis(LocalDateTime dateTime, ZoneOffset offset) {
        return dateTime.toInstant(offset).toEpochMilli();
    }

    public static long toMillis(Duration duration) {
        return duration.toMillis();
    }

    public static LocalDateTime toDateTime(long millis) {
        return toDateTime(millis, ZoneOffset.UTC);
    }

    public static LocalDateTime toDateTime(long millis, ZoneOffset offset) {
        return isEpochSecond(millis) ?
                toDateTime(millis, 0, offset) :
                LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), offset);
    }

    public static LocalDateTime toDateTime(long millis, int nanoOfSecond) {
        return toDateTime(millis, nanoOfSecond, ZoneOffset.UTC);
    }

    public static LocalDateTime toDateTime(long millis, int nanoOfSecond, ZoneOffset offset) {
        return LocalDateTime.ofEpochSecond(millis, nanoOfSecond, offset);
    }

    public static LocalDateTime toDateTime(Duration duration) {
        return toDateTime(duration, ZoneOffset.UTC);
    }

    public static LocalDateTime toDateTime(Duration duration, ZoneOffset offset) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(toMillis(duration)), offset);
    }

    public static Date toDate(LocalDateTime dateTime) {
        return toDate(dateTime, ZoneOffset.UTC);
    }

    public static Date toDate(LocalDateTime dateTime, ZoneOffset offset) {
        return Date.from(dateTime.toInstant(offset));
    }

    public static Duration toDuration(long millis) {
        return Duration.ofMillis(millis);
    }

    public static Duration toDuration(LocalDateTime dateTime) {
        return toDuration(dateTime, ZoneOffset.UTC);
    }

    public static Duration toDuration(LocalDateTime dateTime, ZoneOffset offset) {
        return Duration.ofMillis(toMillis(dateTime, offset));
    }

    public static boolean isEpochSecond(long value) {
        return value < 100_000_000_000L;
    }

    private TimeUtils() {
    }
}
