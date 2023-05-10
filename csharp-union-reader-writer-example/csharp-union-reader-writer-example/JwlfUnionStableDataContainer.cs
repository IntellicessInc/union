using System;
using Newtonsoft.Json;
namespace Union;

public class JwlfUnionStableDataContainer
{
    [JsonProperty("content")]
    public JwlfResponse? Content { get; set; }
    [JsonProperty("stableDataTimestamp")]
    public long? StableDataTimestamp { get; set; }
}
