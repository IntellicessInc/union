package com.intellicess.union.java_union_reader_writer_example.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Objects;

public enum JwlfValueType {

    BOOLEAN("boolean"), STRING("string"), INTEGER("integer"), FLOAT("float"), DATETIME("datetime");

    private final String name;

    JwlfValueType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    @JsonCreator
    public static JwlfValueType ofName(String name) {
        if (name == null) return null;

        return Arrays.stream(values())
                .filter(jwlfalueType -> Objects.equals(jwlfalueType.name, name))
                .findFirst()
                .orElseThrow();
    }
}
