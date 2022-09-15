package com.intellicess.union.java_union_reader_writer_example.config;

import com.intellicess.union.java_union_reader_writer_example.utils.AppConfiguration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Objects;

public class EnvAppConfiguration implements AppConfiguration {

    private static final String LOGGING_LEVEL_ENV = "LOGGING_LEVEL";
    private static final String AUTH_PROVIDER_URL_ENV = "AUTH_PROVIDER_URL";
    private static final String AUTH_PROVIDER_REALM_ENV = "AUTH_PROVIDER_REALM";
    private static final String AUTH_CLIENT_ID_ENV = "AUTH_CLIENT_ID";
    private static final String AUTH_USER_USERNAME_ENV = "AUTH_USER_USERNAME";
    private static final String AUTH_USER_PASSWORD_ENV = "AUTH_USER_PASSWORD";
    private static final String UNION_URL_ENV = "UNION_URL";
    private static final String UNION_CLIENT_ENV = "UNION_CLIENT";
    private static final String UNION_REGION_ENV = "UNION_REGION";
    private static final String UNION_ASSET_ENV = "UNION_ASSET";
    private static final String UNION_FOLDER_ENV = "UNION_FOLDER";
    private static final String LOCAL_WORKING_DIRECTORY_ENV = "LOCAL_WORKING_DIRECTORY";

    private final String authProviderUrl;
    private final String authProviderRealm;
    private final String authClientId;
    private final String authUserUsername;
    private final String authUserPassword;
    private final String unionUrl;
    private final String unionClient;
    private final String unionRegion;
    private final String unionAsset;
    private final String unionFolder;
    private final String localWorkingDirectory;

    private EnvAppConfiguration() {
        this.authProviderUrl = requireEnvVar(AUTH_PROVIDER_URL_ENV);
        this.authProviderRealm = requireEnvVar(AUTH_PROVIDER_REALM_ENV);
        this.authClientId = requireEnvVar(AUTH_CLIENT_ID_ENV);
        this.authUserUsername = requireEnvVar(AUTH_USER_USERNAME_ENV);
        this.authUserPassword = requireEnvVar(AUTH_USER_PASSWORD_ENV);
        this.unionUrl = requireEnvVar(UNION_URL_ENV);
        this.unionClient = requireEnvVar(UNION_CLIENT_ENV);
        this.unionRegion = requireEnvVar(UNION_REGION_ENV);
        this.unionAsset = requireEnvVar(UNION_ASSET_ENV);
        this.unionFolder = requireEnvVar(UNION_FOLDER_ENV);
        this.localWorkingDirectory = requireEnvVar(LOCAL_WORKING_DIRECTORY_ENV);
    }

    public static EnvAppConfiguration init() {
        configureLogging();
        return new EnvAppConfiguration();
    }

    private static void configureLogging() {
        String loggingLevelString = requireEnvVar(LOGGING_LEVEL_ENV);
        Level loggingLevel = Level.toLevel(loggingLevelString);
        Configurator.setRootLevel(loggingLevel);
    }

    public String getAuthProviderUrl() {
        return authProviderUrl;
    }

    public String getAuthProviderRealm() {
        return authProviderRealm;
    }

    public String getAuthClientId() {
        return authClientId;
    }

    public String getAuthUserUsername() {
        return authUserUsername;
    }

    public String getAuthUserPassword() {
        return authUserPassword;
    }

    public String getUnionUrl() {
        return unionUrl;
    }

    public String getUnionClient() {
        return unionClient;
    }

    public String getUnionRegion() {
        return unionRegion;
    }

    public String getUnionAsset() {
        return unionAsset;
    }

    public String getUnionFolder() {
        return unionFolder;
    }

    public String getLocalWorkingDirectory() {
        return localWorkingDirectory;
    }

    private static String requireEnvVar(String name) {
        return Objects.requireNonNull(System.getenv(name), "Environment variable with name '" + name + "' is required!");
    }
}
