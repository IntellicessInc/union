package com.intellicess.union.java_union_reader_writer_example.config;

import com.intellicess.union.java_union_reader_writer_example.utils.AppConfiguration;

public class DevAppConfiguration implements AppConfiguration {

    private final Type type;

    private DevAppConfiguration(Type type) {
        this.type = type;
    }

    public static DevAppConfiguration initReader() {
        return new DevAppConfiguration(Type.READER);
    }

    public static DevAppConfiguration initWriter() {
        return new DevAppConfiguration(Type.WRITER);
    }

    @Override
    public String getAuthProviderUrl() {
        return "https://users.intellicess.com";
    }

    @Override
    public String getAuthProviderRealm() {
        return "public-test";
    }

    @Override
    public String getAuthClientId() {
        return "example-app";
    }

    @Override
    public String getAuthUserUsername() {
        if (type == Type.READER) {
            return ""; // TO FILL: reader username
        } else if (type == Type.WRITER) {
            return ""; // TO FILL: writer username
        }
        throw new RuntimeException("Type='" + type + "' and is not recognized");
    }

    @Override
    public String getAuthUserPassword() {
        if (type == Type.READER) {
            return ""; // TO FILL: reader password
        } else if (type == Type.WRITER) {
            return ""; // TO FILL: writer password
        }
        throw new RuntimeException("Type='" + type + "' and is not recognized");
    }

    @Override
    public String getUnionUrl() {
        return "https://theunion.cloud";
    }

    @Override
    public String getUnionClient() {
        return "public-test";
    }

    @Override
    public String getUnionRegion() {
        return "texas";
    }

    @Override
    public String getUnionAsset() {
        return "well1";
    }

    @Override
    public String getUnionFolder() {
        return "shared-data";
    }

    @Override
    public String getLocalWorkingDirectory() {
        if (type == Type.READER) {
            return "./reader-folder";
        } else if (type == Type.WRITER) {
            return "./writer-folder";
        }
        throw new RuntimeException("Type='" + type + "' and is not recognized");
    }

    private enum Type {
        READER, WRITER
    }
}
