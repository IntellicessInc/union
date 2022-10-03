using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Union;

namespace Union;

public class JwlfSavedResponseList
{
    [JsonProperty("list")]
    public List<JwlfSavedResponse>? List { get; set; }
}
