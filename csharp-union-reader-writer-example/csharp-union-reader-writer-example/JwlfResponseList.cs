using System;
using System.Collections.Generic;

namespace Union;
using Newtonsoft.Json;

public class JwlfResponseList
{

    [JsonProperty("list")]
    public List<JwlfResponse> List { get; set; }

    [JsonProperty("stableDataTimestamp")]
    public long StableDataTimestamp { get; set; }
}
