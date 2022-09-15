package com.intellicess.union.java_union_reader_writer_example.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class DateTimeConverter {

    private static final Logger log = LogManager.getLogger(DateTimeConverter.class);

    private DateTimeConverter(){
    }

    public static ZonedDateTime parseZonedDateTime(String text) {
        ZonedDateTime result;
        try {
            result = ZonedDateTime.parse(text);
        } catch (Exception e1) {
            try {
                result = LocalDateTime.parse(text).atZone(ZoneOffset.UTC);
            } catch (Exception e2) {
                try {
                    result = LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC);
                } catch (Exception e3) {
                    log.error("Couldn't parse value='{}' to ZonedDateTime", text);
                    throw e1;
                }
            }
        }
        return result;
    }
}
