using System;
using Newtonsoft.Json;
namespace Union;

public class JwlfResponse
{
    [JsonProperty("id")]
    public string? Id { get; set; }
    [JsonProperty("client")]
    public string? Client { get; set; }
    [JsonProperty("region")]
    public string? Region { get; set; }
    [JsonProperty("asset")]
    public string? Asset { get; set; }
    [JsonProperty("folder")]
    public string? Folder { get; set; }
    [JsonProperty("creationTimestamp")]
    public ulong? CreationTimestamp { get; set; }
    [JsonProperty("header")]
    public JwlfHeader? Header { get; set; }
    [JsonProperty("curves")]
    public List<JwlfCurve>? Curves { get; set; }
    [JsonProperty("data")]
    public List<List<object>>? Data { get; set; }
}
