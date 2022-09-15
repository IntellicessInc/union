package com.intellicess.union.java_union_reader_writer_example.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeUtils {
    private TimeUtils() {
    }

    public static long dateTimeToMillisecondsTimestamp(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

}
