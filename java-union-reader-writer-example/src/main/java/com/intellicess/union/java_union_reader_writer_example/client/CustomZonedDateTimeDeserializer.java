package com.intellicess.union.java_union_reader_writer_example.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.ZonedDateTime;

public class CustomZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime> {

    private static final Logger log = LogManager.getLogger(CustomZonedDateTimeDeserializer.class);

    @Override
    public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String text = p.getText();
        try {
            return DateTimeConverter.parseZonedDateTime(text);
        } catch (Exception e) {
            log.error("Couldn't deserialize value='{}' to ZonedDateTime", text);
            throw e;
        }
    }
}
