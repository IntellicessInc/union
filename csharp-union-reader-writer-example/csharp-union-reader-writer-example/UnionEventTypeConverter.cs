using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Union;

namespace Union;

public class UnionEventTypeJsonConverter : JsonConverter<UnionEventType?>
{
    public override UnionEventType? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        string? value = reader.GetString();
        if (value == null)
        { 
            return null;
        }
        return (UnionEventType)Enum.Parse(typeof(UnionEventType), value.ToUpperInvariant());
    }

    public override void Write(Utf8JsonWriter writer, UnionEventType? value, JsonSerializerOptions options)
    {
        if (value.HasValue)
        {
            string valueTypeString = Enum.GetName(typeof(UnionEventType), value.Value);
            writer.WriteStringValue(valueTypeString.ToLowerInvariant());
        } else
        {
            writer.WriteNullValue();
        }
    }
}
