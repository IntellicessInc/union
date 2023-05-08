using Microsoft.VisualBasic;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;

namespace Union;

public static class CsvJwlfConverter
{

    private static List<string> TRUE_BOOLEAN_VALUES = new List<string> { "yes", "true", "on" };
    private static List<string> FALSE_BOOLEAN_VALUES = new List<string> { "no", "false", "off" };
    public static string FILENAME_METADATA_KEY = "filename";

    private static Logger log = AppLogFactory.CreateLogger();


    public static JwlfRequest ConvertCsvToJwlf(string well, string filename, string content,
                                               Dictionary<string, JwlfValueType> headerValueTypeMap)
    {
        string filenameWithoutExtension = filename.Replace(".csv", "");
        JwlfRequest jwlfRequest = new JwlfRequest();
        JwlfHeader jwlfHeader = new JwlfHeader
        {
            Name = filenameWithoutExtension,
            Well = well
        };
        jwlfRequest.Header = jwlfHeader;
        if (content == null || String.IsNullOrWhiteSpace(content))
        {
            return jwlfRequest;
        }

        List<string> lines = content.Split("\n").ToList();
        string titleLine = lines.First();
        List<string> headers = titleLine.Split(",").ToList();
        List<JwlfCurve> jwlfCurves = headers.Select(header => {
            JwlfValueType? valueType = null;
            if (headerValueTypeMap.ContainsKey(header))
            {
                valueType = headerValueTypeMap[header];
            }
            return new JwlfCurve
            {
                Name = header,
                ValueType = valueType
            };
        })
            .ToList();
        jwlfRequest.Curves = jwlfCurves;
        if (lines.Count() == 1)
        {
            return jwlfRequest;
        }

        List<string> dataLines = lines.Skip(1).ToList();
        List<List<Object>> data = dataLines.Select(dataLine => ConvertCsvLineToJwlfData(dataLine, jwlfCurves)).ToList();
        jwlfRequest.Data = data;
        return jwlfRequest;
    }

    private static List<Object> ConvertCsvLineToJwlfData(string dataLine, List<JwlfCurve> jwlfCurves)
    {
        List<string> values = dataLine.Split(",").ToList();
        if (values.Count() != jwlfCurves.Count())
        {
            throw new Exception("Data line='" + dataLine + "' should have the same number of values as number of headers ('" + jwlfCurves.Count() + "').");
        }
        List<Object> castedValues = new List<Object>();
        for (int i = 0; i < values.Count(); i++)
        {
            JwlfCurve jwlfCurve = jwlfCurves[i];
            Object value = values[i];
            Object castedValue = CastValueToRightType(jwlfCurve, value);
            castedValues.Add(castedValue);
        }
        return castedValues;
    }

    public static Object CastValueToRightType(JwlfCurve jwlfCurve, Object value)
    {
        if (value == null)
        {
            return null;
        }

        JwlfValueType? jwlfValueType = jwlfCurve.ValueType;
        if (!jwlfValueType.HasValue)
        {
            return value;
        }

        string stringValue = value.ToString();
        switch (jwlfValueType)
        {
            case JwlfValueType.STRING:
            case JwlfValueType.DATETIME:
                return stringValue;
            case JwlfValueType.INTEGER:
                return int.Parse(stringValue);
            case JwlfValueType.FLOAT:
                return double.Parse(stringValue, CultureInfo.GetCultureInfo("nl-NL"));
            case JwlfValueType.BOOLEAN:
                string lowerCasestringValue = stringValue.ToLowerInvariant();
                if (TRUE_BOOLEAN_VALUES.Contains(lowerCasestringValue))
                {
                    return true;
                }
                else if (FALSE_BOOLEAN_VALUES.Contains(lowerCasestringValue))
                {
                    return false;
                }
                break;
        }
        log.Warning("Couldn't cast value='{Value}' to value type='{JwlfValueType}'. Returning null for curve with name='{JwlfCurveName}'",
                value, jwlfValueType, jwlfCurve.Name);
        return null;
    }
}

