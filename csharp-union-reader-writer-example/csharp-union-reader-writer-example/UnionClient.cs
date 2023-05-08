namespace Union;

using Union;
using System;
using Serilog.Core;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Web;

public class UnionClient
{

    private const ulong MAX_ACCESS_TOKEN_AGE_SECONDS = 60;
    private const int MAX_NUMBER_OF_IDS_PER_REQUEST = 25;
    private const string APPLICATION_X_WWW_FORM_URLENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded";
    private const string ACCESS_TOKEN_BEARER_PREFIX = "Bearer ";
	private static Logger log = AppLogFactory.CreateLogger();

    private string _lastAccessToken;
    private DateTime? _accessTokenCreationTime = null;
    private ConcurrentDictionary<string, long> _searchSinceTimestampDict = new ConcurrentDictionary<string, long>();
    
    private readonly UnionClientConfiguration _config;
    private readonly HttpClient _httpClient;


    public UnionClient(UnionClientConfiguration config)
    {
        _config = config;
        _httpClient = new HttpClient();
    }

    public JwlfSavedResponse SaveJwlfLog(string client, string region, string asset, string folder, JwlfRequest jwlfLog)
    {
        string url = _config.UnionUrl + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;
        string accessToken = GetAccessToken();

        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JwlfValueTypeJsonConverter());

        var jsonContent =  JsonContent.Create(jwlfLog, mediaType: null, options);
        _httpClient.DefaultRequestHeaders.Authorization = AuthenticationHeaderValue.Parse(ACCESS_TOKEN_BEARER_PREFIX + accessToken);
        var response =  _httpClient.PostAsync(url, jsonContent).Result;
        var content = response.Content;
        if ((int)response.StatusCode >= 300)
        {
            throw new Exception("UnionClient#SaveJwlfLog failure with http response status code=" + response.StatusCode
                    + " and response=" + content.ReadAsStringAsync().Result);
        }
        JwlfSavedResponse responseBody = content.ReadFromJsonAsync<JwlfSavedResponse>(options).Result;
        return responseBody;
    }

    public JwlfSavedResponseList SaveJwlfLogs(string client, string region, string asset, string folder, List<JwlfRequest> jwlfLogs)
    {
        string url = _config.UnionUrl + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder + "/batch-save";
        string accessToken = GetAccessToken();

        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JwlfValueTypeJsonConverter());
        
        var jsonContent =  JsonContent.Create(jwlfLogs, mediaType: null, options);
		log.Information("sending jsonContent={jsonContent}", jsonContent.ToString());
        _httpClient.DefaultRequestHeaders.Authorization = AuthenticationHeaderValue.Parse(ACCESS_TOKEN_BEARER_PREFIX + accessToken);
        var response =  _httpClient.PostAsync(url, jsonContent).Result;
        var content = response.Content;
        if ((int)response.StatusCode >= 300)
        {
            throw new Exception("UnionClient#SaveJwlfLogs failure with http response status code=" + response.StatusCode
                    + " and response=" + content.ToString());
        }
        JwlfSavedResponseList responseBody = content.ReadFromJsonAsync<JwlfSavedResponseList>(options).Result;
        return responseBody;
    }

    public JwlfResponseList GetNewJwlfLogsWithData(string client, string region, string asset, string folder,
                                                        long? inclusiveSinceTimestamp)
    {
        string searchKey = client + "/" + region + "/" + asset + "/" + folder;
        long inclusiveSinceTimestampValue = inclusiveSinceTimestamp.GetValueOrDefault(0);
        long currentInclusiveSinceTimestamp = _searchSinceTimestampDict.GetValueOrDefault(searchKey, inclusiveSinceTimestampValue);

        long stableDataTimestamp = GetStableJwlfDataTimestamp(client, region, asset, folder);
        long tillTimestampExclusive = stableDataTimestamp + 1;

        JwlfResponseList jwlfLogsWithoutData = GetJwlfLogsWithoutData(client, region, asset, folder,
                currentInclusiveSinceTimestamp, tillTimestampExclusive);
        List<JwlfResponse> logsWithoutData = jwlfLogsWithoutData.List;
        List<string> ids = logsWithoutData.Select(jwlfLog => jwlfLog.Id).ToList();

        _searchSinceTimestampDict[searchKey] = tillTimestampExclusive;
        if (ids.Count() == 0) {
            return new JwlfResponseList
            {
                List = new List<JwlfResponse>(),
                StableDataTimestamp = jwlfLogsWithoutData.StableDataTimestamp
            };
        }
        return GetJwlfLogsWithData(client, region, asset, folder, ids);
    }

    public JwlfResponseList GetJwlfLogsWithData(string client, string region, string asset, string folder,
                                                List<string> ids) {
        List<List<string>> idsBatches = new List<List<string>>();
        idsBatches.Add(new List<string>());
        int idBatchIndex = 0;
        int idIndexInBatch = 0;
        foreach (string jwlfId in ids) {
            if (idIndexInBatch >= MAX_NUMBER_OF_IDS_PER_REQUEST) {
                idsBatches.Add(new List<string>());
                idBatchIndex++;
                idIndexInBatch = 0;
            }
            List<string> idsBatch = idsBatches[idBatchIndex];
            idsBatch.Add(jwlfId);
            idIndexInBatch++;
        }

        string baseUrl = _config.UnionUrl + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;
        List<JwlfResponse> jwlfLogs = new List<JwlfResponse>();
        long stableDataTimestamp = 0;
        foreach (List<string> idsBatch in idsBatches) {
            string url = baseUrl + "?" + string.Join("&", idsBatch.Select(id => "id=" + HttpUtility.UrlEncode(id)));
            string accessToken = GetAccessToken();
            var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
            options.Converters.Add(new JwlfValueTypeJsonConverter());
            options.Converters.Add(new ObjectDeserializer());

            _httpClient.DefaultRequestHeaders.Authorization = AuthenticationHeaderValue.Parse(ACCESS_TOKEN_BEARER_PREFIX + accessToken);
            var response = _httpClient.GetAsync(url).Result;
            var content = response.Content;
            if ((int)response.StatusCode >= 300)
            {
                throw new Exception("UnionClient#GetJwlfLogsWithData failure with http response status code=" + response.StatusCode
                        + " and response=" + content.ReadAsStringAsync().Result);
            }
            JwlfResponseList jwlfResponseList = content.ReadFromJsonAsync<JwlfResponseList>(options).Result;
            jwlfLogs.AddRange(jwlfResponseList.List);
            stableDataTimestamp = jwlfResponseList.StableDataTimestamp;
        }
        return new JwlfResponseList
        {
            List = jwlfLogs,
            StableDataTimestamp = stableDataTimestamp
        };
    }
    
    public JwlfResponseList GetJwlfLogsWithoutData(String client, String region, String asset, String folder,
                                                   long sinceTimestampInclusive, long tillTimestampExclusive) {
        string baseUrl = _config.UnionUrl + "/api/v1/well-logs/" + client + "/" + region + "/" + asset + "/" + folder;
        string sinceTimestampInclusiveUrlEncoded = HttpUtility.UrlEncode(sinceTimestampInclusive.ToString());
        string url = baseUrl + "?sinceTimestamp=" + sinceTimestampInclusiveUrlEncoded + "&tillTimestamp=" + HttpUtility.UrlEncode(tillTimestampExclusive.ToString());
        string accessToken = GetAccessToken();
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JwlfValueTypeJsonConverter());
        options.Converters.Add(new ObjectDeserializer());

        _httpClient.DefaultRequestHeaders.Authorization = AuthenticationHeaderValue.Parse(ACCESS_TOKEN_BEARER_PREFIX + accessToken);
        var response =  _httpClient.GetAsync(url).Result;
        var content = response.Content;
        if ((int)response.StatusCode >= 300)
        {
            throw new Exception("UnionClient#GetJwlfLogsWithoutData failure with http response status code=" + response.StatusCode
                    + " and response=" + content.ReadAsStringAsync().Result);
        }
        return content.ReadFromJsonAsync<JwlfResponseList>(options).Result;
    }

    public long GetStableJwlfDataTimestamp(string client, string region, string asset, string folder) {
        try {
            JwlfResponseList jwlfLogsWithoutData = GetJwlfLogsWithoutData(client, region, asset, folder, 0, 0);
            return jwlfLogsWithoutData.StableDataTimestamp;
        } catch (Exception e) {
            throw new Exception("UnionClient#getStableJwlfDataTimestamp failure caused by the following exception", e);
        }
    }

    private string GetAccessToken()
    {
        ulong accessTokenAgeSeconds = MAX_ACCESS_TOKEN_AGE_SECONDS;
        if (_accessTokenCreationTime.HasValue)
        {
            accessTokenAgeSeconds = (ulong)((DateTime.Now - _accessTokenCreationTime.Value).TotalSeconds);
        }
        if (accessTokenAgeSeconds < MAX_ACCESS_TOKEN_AGE_SECONDS) {
            return _lastAccessToken;
        }
        var form = new Dictionary<string, string>
        {
            { "client_id", _config.AuthClientId },
            { "grant_type", "password" },
            { "username", _config.Username },
            { "password", _config.Password },
        };
        string url = _config.AuthProviderUrl + "/auth/realms/" + _config.AuthProviderRealm + "/protocol/openid-connect/token";

        var response = _httpClient.PostAsync(url, new FormUrlEncodedContent(form)).Result;
        var content = response.Content;
        if ((int)response.StatusCode >= 300)
        {
            throw new Exception("UnionClient#GetAccessToken failure with http response status code="
                    + response.StatusCode + " and response=" + content.ToString());
        }
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        AccessTokenResponse accessTokenResponse = content.ReadFromJsonAsync<AccessTokenResponse>(options).Result;
        _lastAccessToken = accessTokenResponse.AccessToken;
        _accessTokenCreationTime = DateTime.Now;
        return _lastAccessToken;
    }
}
