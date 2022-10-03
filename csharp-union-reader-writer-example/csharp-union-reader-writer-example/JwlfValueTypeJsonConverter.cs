using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Union;

namespace Union;

public class JwlfValueTypeJsonConverter : JsonConverter<JwlfValueType?>
{
    public override JwlfValueType? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        string? value = reader.GetString();
        if (value == null)
        { 
            return null;
        }
        return (JwlfValueType)Enum.Parse(typeof(JwlfValueType), value.ToUpperInvariant());
    }

    public override void Write(Utf8JsonWriter writer, JwlfValueType? value, JsonSerializerOptions options)
    {
        if (value.HasValue)
        {
            string valueTypeString = Enum.GetName(typeof(JwlfValueType), value.Value);
            writer.WriteStringValue(valueTypeString.ToLowerInvariant());
        } else
        {
            writer.WriteNullValue();
        }
    }
}
