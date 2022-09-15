package com.intellicess.union.java_union_reader_writer_example.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.intellicess.union.java_union_reader_writer_example.client.CustomZonedDateTimeDeserializer;

import java.time.ZonedDateTime;

public class ObjectMapperFactory {

    private static ObjectMapper INSTANCE;

    private ObjectMapperFactory() {
    }

    public static ObjectMapper createObjectMapper() {
        if (INSTANCE == null) {
            SimpleModule customZonedDateTimeModule = new SimpleModule("CustomZonedDateTimeModule");
            customZonedDateTimeModule.addDeserializer(ZonedDateTime.class, new CustomZonedDateTimeDeserializer());
            INSTANCE = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .registerModule(new JavaTimeModule())
                    .registerModule(customZonedDateTimeModule);
        }
        return INSTANCE;
    }
}
