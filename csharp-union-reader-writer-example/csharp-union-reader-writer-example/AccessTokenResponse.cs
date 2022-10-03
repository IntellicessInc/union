using System;
using System.Text.Json.Serialization;

namespace Union;

class AccessTokenResponse
{
    [JsonPropertyName("access_token")]
    public string? AccessToken { get; set; }
    [JsonPropertyName("expires_in")]
    public ulong? ExpiresIn { get; set; }
    [JsonPropertyName("refresh_expires_in")]
    public ulong? RefreshExpiresIn { get; set; }
    [JsonPropertyName("refresh_token")]
    public string? RefreshToken { get; set; }
    [JsonPropertyName("token_type")]
    public string? TokenType { get; set; }
    [JsonPropertyName("session_state")]
    public string? SessionState { get; set; }
}
