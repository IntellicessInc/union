using Newtonsoft.Json;

namespace Union;

public class JwlfHeader
{
    [JsonProperty("name")]
    public string Name { get; set; } = "";
    [JsonProperty("description")]
    public string? Description { get; set; }
    [JsonProperty("well")]
    public string? Well { get; set; }
    [JsonProperty("wellbore")]
    public string? Wellbore { get; set; }
    [JsonProperty("field")]
    public string? Field { get; set; }
    [JsonProperty("country")]
    public string? Country { get; set; }
    [JsonProperty("date")]
    public string? Date { get; set; }
    [JsonProperty("operator")]
    public string? JwlfOperator { get; set; }
    [JsonProperty("serviceCompany")]
    public string? ServiceCompany { get; set; }
    [JsonProperty("runNumber")]
    public string? RunNumber { get; set; }
    [JsonProperty("elevation")]
    public double? Elevation { get; set; }
    [JsonProperty("source")]
    public string? Source { get; set; }
    [JsonProperty("startIndex")]
    public double? StartIndex { get; set; }
    [JsonProperty("endIndex")]
    public double? EndIndex { get; set; }
    [JsonProperty("step")]
    public double? Step { get; set; }
    [JsonProperty("dataUri")]
    public string? DataUri { get; set; }
    [JsonProperty("metadata")]
    public Dictionary<string, string>? Metadata { get; set; } = new Dictionary<string, string>();
}
