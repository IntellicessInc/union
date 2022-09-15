package com.intellicess.union.java_union_reader_writer_example.client;

public class UnionClientConfiguration {
    public String getUnionUrl() {
        return unionUrl;
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

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    private final String unionUrl;
    private final String authProviderUrl;
    private final String authProviderRealm;
    private final String authClientId;
    private final String username;
    private final String password;

    public UnionClientConfiguration(String unionUrl, String authProviderUrl, String authProviderRealm, String authClientId,
                                    String username, String password) {
        this.unionUrl = unionUrl;
        this.authProviderUrl = authProviderUrl;
        this.authProviderRealm = authProviderRealm;
        this.authClientId = authClientId;
        this.username = username;
        this.password = password;
    }

    public static UnionClientConfigurationBuilder builder() {
        return new UnionClientConfigurationBuilder();
    }

    public static final class UnionClientConfigurationBuilder {
        private String unionUrl;
        private String authProviderUrl;
        private String authProviderRealm;
        private String authClientId;
        private String username;
        private String password;

        private UnionClientConfigurationBuilder() {
        }

        public UnionClientConfigurationBuilder unionUrl(String unionUrl) {
            this.unionUrl = unionUrl;
            return this;
        }

        public UnionClientConfigurationBuilder authProviderUrl(String authProviderUrl) {
            this.authProviderUrl = authProviderUrl;
            return this;
        }

        public UnionClientConfigurationBuilder authProviderRealm(String authProviderRealm) {
            this.authProviderRealm = authProviderRealm;
            return this;
        }

        public UnionClientConfigurationBuilder authClientId(String authClientId) {
            this.authClientId = authClientId;
            return this;
        }

        public UnionClientConfigurationBuilder username(String username) {
            this.username = username;
            return this;
        }

        public UnionClientConfigurationBuilder password(String password) {
            this.password = password;
            return this;
        }

        public UnionClientConfiguration build() {
            return new UnionClientConfiguration(unionUrl, authProviderUrl, authProviderRealm, authClientId, username, password);
        }
    }
}
