using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Union;

public class JwlfCurve
{
    [JsonProperty("name")]
    public string? Name { get; set; } = "";
    [JsonProperty("description")]
    public string? Description { get; set; }
    [JsonProperty("quantity")]
    public string? Quantity { get; set; }
    [JsonProperty("unit")]
    public string? Unit { get; set; }
    [JsonProperty("valueType")]
    public JwlfValueType? ValueType { get; set; }
    [JsonProperty("dimensions")]
    public ulong? Dimensions { get; set; } = 1;
    [JsonProperty("axis")]
    public List<object>? Axis { get; set; }
    [JsonProperty("maxSize")]
    public ulong? MaxSize { get; set; }
}
