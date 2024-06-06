package io.github.regychang.flinkify.common.utils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeUtilsTest {

    @Test
    void toMillisLocalDateTime() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.MAY, 17, 23, 40);
        long millis = TimeUtils.toMillis(dateTime);
        assertEquals(1684366800000L, millis);
    }

    @Test
    void toMillisLocalDateTimeWithOffset() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.MAY, 18, 10, 0);
        long millis = TimeUtils.toMillis(dateTime, ZoneOffset.ofHours(5));
        long expectedMillis = dateTime.toInstant(ZoneOffset.ofHours(5)).toEpochMilli();
        assertEquals(expectedMillis, millis);
    }

    @Test
    void toMillisDuration() {
        Duration duration = Duration.ofHours(10);
        long millis = TimeUtils.toMillis(duration);
        assertEquals(36000000L, millis);
    }

    @Test
    void toDateTimeMillis() {
        long millis = 1684366800000L;
        LocalDateTime dateTime = TimeUtils.toDateTime(millis);
        LocalDateTime expectedDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        assertEquals(expectedDateTime, dateTime);
    }

    @Test
    void toDateTimeMillisWithOffset() {
        long millis = 1684384800000L;
        LocalDateTime dateTime = TimeUtils.toDateTime(millis, ZoneOffset.ofHours(5));
        LocalDateTime expectedDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.ofHours(5));
        assertEquals(expectedDateTime, dateTime);
    }

    @Test
    void toDateTimeMillisAndNano() {
        long millis = 1684366800L;
        int nanoOfSecond = 500;
        LocalDateTime dateTime = TimeUtils.toDateTime(millis, nanoOfSecond);
        LocalDateTime expectedDateTime = LocalDateTime.ofEpochSecond(millis, nanoOfSecond, ZoneOffset.UTC);
        assertEquals(expectedDateTime, dateTime);
    }

    @Test
    void toDateTimeMillisAndNanoWithOffset() {
        long millis = 1684366800L;
        int nanoOfSecond = 500;
        LocalDateTime dateTime = TimeUtils.toDateTime(millis, nanoOfSecond, ZoneOffset.ofHours(5));
        LocalDateTime expectedDateTime = LocalDateTime.ofEpochSecond(millis, nanoOfSecond, ZoneOffset.ofHours(5));
        assertEquals(expectedDateTime, dateTime);
    }

    @Test
    void toDateTimeDuration() {
        Duration duration = Duration.ofHours(10);
        LocalDateTime dateTime = TimeUtils.toDateTime(duration);
        LocalDateTime expectedDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(36000000L), ZoneOffset.UTC);
        assertEquals(expectedDateTime, dateTime);
    }

    @Test
    void toDate() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.OCTOBER, 15, 10, 0);
        Date date = TimeUtils.toDate(dateTime);
        assertEquals(Date.from(dateTime.toInstant(ZoneOffset.UTC)), date);
    }

    @Test
    void toDateWithOffset() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.OCTOBER, 15, 10, 0);
        Date date = TimeUtils.toDate(dateTime, ZoneOffset.ofHours(5));
        assertEquals(Date.from(dateTime.toInstant(ZoneOffset.ofHours(5))), date);
    }

    @Test
    void toDurationMillis() {
        long millis = 36000000L;
        Duration duration = TimeUtils.toDuration(millis);
        assertEquals(Duration.ofHours(10), duration);
    }

    @Test
    void toDurationDateTime() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.OCTOBER, 15, 10, 0);
        Duration duration = TimeUtils.toDuration(dateTime);
        assertEquals(Duration.ofMillis(dateTime.toInstant(ZoneOffset.UTC).toEpochMilli()), duration);
    }

    @Test
    void toDurationDateTimeWithOffset() {
        LocalDateTime dateTime = LocalDateTime.of(2023, Month.OCTOBER, 15, 10, 0);
        Duration duration = TimeUtils.toDuration(dateTime, ZoneOffset.ofHours(5));
        assertEquals(Duration.ofMillis(dateTime.toInstant(ZoneOffset.ofHours(5)).toEpochMilli()), duration);
    }

    @Test
    void isEpochSecond() {
        assertTrue(TimeUtils.isEpochSecond(9999999999L));
        assertFalse(TimeUtils.isEpochSecond(100000000000L));
    }
}
