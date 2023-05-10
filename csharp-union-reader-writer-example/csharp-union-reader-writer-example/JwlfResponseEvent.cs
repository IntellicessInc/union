using System;
using Newtonsoft.Json;
namespace Union;

public class JwlfResponseEvent
{
    [JsonProperty("id")]
    public string? Id { get; set; }
    [JsonProperty("data")]
    public JwlfUnionStableDataContainer? Data { get; set; }
    [JsonProperty("event")]
    public UnionEventType? Event { get; set; }
    [JsonProperty("retry")]
    public int? Retry { get; set; }
}
