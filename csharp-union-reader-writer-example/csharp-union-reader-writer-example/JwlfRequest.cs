namespace Union;
using Newtonsoft.Json;

public class JwlfRequest
{
    [JsonProperty("header")]
    public JwlfHeader? Header { get; set; }
    [JsonProperty("curves")]
    public List<JwlfCurve>? Curves { get; set; }
    [JsonProperty("data")]
    public List<List<Object>>? Data { get; set; }
}
