package com.intellicess.union.java_union_reader_writer_example.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intellicess.union.java_union_reader_writer_example.client.model.*;
import com.intellicess.union.java_union_reader_writer_example.client.model.auth.AccessTokenResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UnionClient {

    private static final long MAX_ACCESS_TOKEN_AGE_SECONDS = 60;
    private static final int MAX_NUMBER_OF_IDS_PER_REQUEST = 25;
    private static final String APPLICATION_X_WWW_FORM_URLENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    public static final String ACCESS_TOKEN_BEARER_PREFIX = "Bearer ";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final UnionClientConfiguration config;

    private String lastAccessToken;
    private LocalDateTime accessTokenCreationTime;
    private final Map<String, Long> searchSinceTimestampMap = new ConcurrentHashMap<>();

    public UnionClient(UnionClientConfiguration unionClientConfiguration, ObjectMapper objectMapper) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = objectMapper;
        this.config = unionClientConfiguration;
    }

    public JwlfSavedResponse saveJwlfLog(String client, String region, String asset, String folder, JwlfRequest jwlfLog) {
        String json = toJson(jwlfLog);
        String url = config.getUnionUrl() + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;
        String accessToken = getAccessToken();
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(URI.create(url))
                .header("Authorization", ACCESS_TOKEN_BEARER_PREFIX + accessToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = sendRequest(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = response.body();
        if (response.statusCode() >= 300) {
            throw new RuntimeException("UnionClient#saveJwlfLog failure with http response status code=" + response.statusCode()
                    + " and response=" + responseBody);
        }
        return fromJson(responseBody, JwlfSavedResponse.class);
    }

    public JwlfSavedResponseList saveJwlfLogs(String client, String region, String asset, String folder,
                                              List<JwlfRequest> jwlfLogs) {
        String json = toJson(jwlfLogs);
        String url = config.getUnionUrl() + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder
                + "/batch-save";
        String accessToken = getAccessToken();
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + accessToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        HttpResponse<String> response = sendRequest(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = response.body();
        if (response.statusCode() >= 300) {
            throw new RuntimeException("UnionClient#saveJwlfLogs failure with http response status code="
                    + response.statusCode() + " and response=" + responseBody);
        }
        return fromJson(responseBody, JwlfSavedResponseList.class);

    }

    public JwlfResponseList getNewJwlfLogsWithData(String client, String region, String asset, String folder,
                                                   Long inclusiveSinceTimestamp) {
        String searchKey = client + "/" + region + "/" + asset + "/" + folder;
        Long currentInclusiveSinceTimestamp = searchSinceTimestampMap.getOrDefault(searchKey, inclusiveSinceTimestamp);

        long stableDataTimestamp = getStableJwlfDataTimestamp(client, region, asset, folder);
        long tillTimestampExclusive = stableDataTimestamp + 1;

        JwlfResponseList jwlfLogsWithoutData = getJwlfLogsWithoutData(client, region, asset, folder,
                currentInclusiveSinceTimestamp, tillTimestampExclusive);
        List<JwlfResponse> logsWithoutData = jwlfLogsWithoutData.getList();
        List<String> ids = logsWithoutData.stream()
                .map(JwlfResponse::getId)
                .collect(Collectors.toList());

        searchSinceTimestampMap.put(searchKey, tillTimestampExclusive);
        if (ids.size() == 0) {
            JwlfResponseList jwlfResponseList = new JwlfResponseList();
            jwlfResponseList.setList(Collections.emptyList());
            jwlfResponseList.setStableDataTimestamp(jwlfLogsWithoutData.getStableDataTimestamp());
            return jwlfResponseList;
        }
        return getJwlfLogsWithData(client, region, asset, folder, ids);
    }

    public JwlfResponseList getJwlfLogsWithData(String client, String region, String asset, String folder,
                                                Collection<String> ids) {
        List<List<String>> idsBatches = new ArrayList<>();
        idsBatches.add(new ArrayList<>());
        int idBatchIndex = 0;
        int idIndexInBatch = 0;
        for (String jwlfId : ids) {
            if (idIndexInBatch >= MAX_NUMBER_OF_IDS_PER_REQUEST) {
                idsBatches.add(new ArrayList<>());
                idBatchIndex++;
                idIndexInBatch = 0;
            }
            List<String> idsBatch = idsBatches.get(idBatchIndex);
            idsBatch.add(jwlfId);
            idIndexInBatch++;
        }

        String baseUrl = config.getUnionUrl() + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;
        List<JwlfResponse> jwlfLogs = new ArrayList<>();
        long stableDataTimestamp = 0L;
        for (List<String> idsBatch : idsBatches) {
            String url = baseUrl + "?" + mapToUrlEncodedForm(Map.of("id", idsBatch));
            String accessToken = getAccessToken();
            HttpRequest request = HttpRequest
                    .newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            HttpResponse<String> response = sendRequest(request, HttpResponse.BodyHandlers.ofString());
            String responseBody = response.body();
            if (response.statusCode() >= 300) {
                throw new RuntimeException("UnionClient#getJwlfLogsWithData failure with http response status code="
                        + response.statusCode() + " and response=" + responseBody);
            }
            JwlfResponseList jwlfResponseList = fromJson(responseBody, JwlfResponseList.class);
            jwlfLogs.addAll(jwlfResponseList.getList());
            stableDataTimestamp = jwlfResponseList.getStableDataTimestamp();
        }
        JwlfResponseList jwlfResponseList = new JwlfResponseList();
        jwlfResponseList.setList(jwlfLogs);
        jwlfResponseList.setStableDataTimestamp(stableDataTimestamp);
        return jwlfResponseList;
    }

    public JwlfResponseList getJwlfLogsWithoutData(String client, String region, String asset, String folder,
                                                   Long sinceTimestampInclusive, Long tillTimestampExclusive) {
        String baseUrl = config.getUnionUrl() + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;

        Map<String, ?> queryParams = Map.of("sinceTimestamp", sinceTimestampInclusive, "tillTimestamp", tillTimestampExclusive);
        String url = baseUrl + "?" + mapToUrlEncodedForm(queryParams);
        String accessToken = getAccessToken();
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();
        HttpResponse<String> response;
        response = sendRequest(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = response.body();
        if (response.statusCode() >= 300) {
            throw new RuntimeException("UnionClient#getJwlfLogsWithoutData failure with http response status code="
                    + response.statusCode() + " and response=" + responseBody);
        }
        return fromJson(responseBody, JwlfResponseList.class);
    }

    public long getStableJwlfDataTimestamp(String client, String region, String asset, String folder) {
        try {
            JwlfResponseList jwlfLogsWithoutData = getJwlfLogsWithoutData(client, region, asset, folder, 0L, 0L);
            return jwlfLogsWithoutData.getStableDataTimestamp();
        } catch (RuntimeException e) {
            throw new RuntimeException("UnionClient#getStableJwlfDataTimestamp failure caused by the following exception", e);
        }
    }

    private String getAccessToken() {
        long accessTokenAgeSeconds = Optional.ofNullable(accessTokenCreationTime)
                .map(creationTime -> ChronoUnit.SECONDS.between(creationTime, LocalDateTime.now()))
                .orElse(MAX_ACCESS_TOKEN_AGE_SECONDS);
        if (accessTokenAgeSeconds < MAX_ACCESS_TOKEN_AGE_SECONDS) {
            return lastAccessToken;
        }
        String url = config.getAuthProviderUrl() + "/auth/realms/" + config.getAuthProviderRealm() + "/protocol/openid-connect/token";
        String urlEncodedForm = mapToUrlEncodedForm(Map.of(
                // "client_secret", clientSecretIfSetupInAuthProviderForClient,
                // "totp", totpFromUserPhoneIf2FAIsSetup,
                "client_id", config.getAuthClientId(),
                "grant_type", "password",
                "username", config.getUsername(),
                "password", config.getPassword()
        ));
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(URI.create(url))
                .header(CONTENT_TYPE_HEADER_NAME, APPLICATION_X_WWW_FORM_URLENCODED_CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(urlEncodedForm))
                .build();
        HttpResponse<String> response = sendRequest(request, HttpResponse.BodyHandlers.ofString());
        String responseBody = response.body();
        if (response.statusCode() >= 300) {
            throw new RuntimeException("UnionClient#getAccessToken failure with http response status code="
                    + response.statusCode() + " and response=" + responseBody);
        }
        AccessTokenResponse accessTokenResponse = fromJson(responseBody, AccessTokenResponse.class);
        lastAccessToken = accessTokenResponse.getAccessToken();
        accessTokenCreationTime = LocalDateTime.now();
        return lastAccessToken;
    }

    private <T> HttpResponse<T> sendRequest(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
        HttpResponse<T> response;
        try {
            response = httpClient.send(request, responseBodyHandler);
        } catch (IOException | InterruptedException e) {
            if (e.getMessage().contains("GOAWAY")) {
                try {
                    response = httpClient.send(request, responseBodyHandler);
                } catch (IOException | InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                throw new RuntimeException(e);
            }
        }
        return response;
    }

    private String mapToUrlEncodedForm(Map<String, ?> map) {
        return map.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .map(this::mapEntryToUrlEncodedForm)
                .flatMap(Collection::stream)
                .collect(Collectors.joining("&"));
    }

    private List<String> mapEntryToUrlEncodedForm(Map.Entry<String, ?> entry) {
        if (entry.getValue() instanceof Collection) {
            return (List<String>)
                    ((Collection) entry.getValue()).stream()
                            .filter(Objects::nonNull)
                            .map(valueElement -> mapEntryToUrlEncodedForm(Map.entry(entry.getKey(), valueElement)))
                            .flatMap(o -> ((List<String>) o).stream())
                            .collect(Collectors.toList());
        }
        String stringValue = String.valueOf(entry.getValue());
        return List.of(entry.getKey() + "=" + URLEncoder.encode(stringValue, StandardCharsets.UTF_8));
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T fromJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
