using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Union;

public class JwlfSavedResponse
{
    [JsonProperty("id")]
    public string? Id { get; set; }
    [JsonProperty("creationTimestamp")]
    public ulong? CreationTimestamp { get; set; }
}
