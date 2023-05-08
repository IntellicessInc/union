using System;
using Serilog.Core;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Linq;
using System.Text.Json;

namespace Union;

public static class CsvJwlfWithBase64EncodedBinariesConverter
{
	private static Logger log = AppLogFactory.CreateLogger();

	private static string BINARIES_DATA_CURVE_NAME = "Binaries data";
    private static string BINARIES_NAMES_CURVE_NAME = "Binaries names";
    public static string BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY = "base64-encoded-binaries-example";


    public static JwlfRequest ConvertFolderToJwlf(string folderPath, Dictionary<string, JwlfValueType> headerValueTypeMapping) {
        string jwlfHeaderName = null;
		Dictionary<string, object> jwlfHeaderValuesMap = new Dictionary<string, object>();
        List<JwlfCurve> jwlfCurves = new List<JwlfCurve>();
        List<object> dataEntry = new List<object>();
        List<string> encodedBinaryFilesContents = new List<string>();
        List<string> encodedBinaryFilesNames = new List<string>();
		var directoryInfo = new DirectoryInfo(@folderPath);
		foreach (FileInfo fileInfo in directoryInfo.GetFiles()) {
            if (Directory.Exists(fileInfo.FullName)) {
                continue;
            }
            string filename = fileInfo.Name;
			string filePath = fileInfo.FullName;
			if (filename.EndsWith(".csv")) {
                jwlfHeaderName = filename.Replace(".csv", "");
				List<string> lines = new List<string>(File.ReadAllLines(@filePath));
                if (lines.Count != 2) {
                    return null;
                }
                List<string> headers = new List<string>(lines[0].Split(","));
                List<string> dataElements = new List<string>(lines[1].Split(","));

				for (int i = 0; i < headers.Count; i++) {
                    string header = headers[i];
					string curveUnit = null;
					string data = dataElements[i];
                    string match = FindMatch(header, "\\[.+]");
                    if (match != null) {
						string fieldName = match.Replace("[", "").Replace("]", "");
                        jwlfHeaderValuesMap.Add(fieldName, data);
                        continue;
                    }
                    string curveUnitMatch = FindMatch(header, "\\(.+\\)");
                    if (curveUnitMatch != null) {
                        curveUnit = curveUnitMatch.Replace("(", "").Replace(")", "");
                        header = header.Replace(curveUnitMatch, "");
                    }
                    JwlfCurve jwlfCurve = new JwlfCurve()
                    {
						Name = header,
						ValueType = headerValueTypeMapping.ContainsKey(header) ? headerValueTypeMapping[header] : null,
						Dimensions = 1,
						Unit = curveUnit
					};
                    jwlfCurves.Add(jwlfCurve);
                    dataEntry.Add(CsvJwlfConverter.CastValueToRightType(jwlfCurve, data));
                }
            } else {
                byte[] contentBytes = File.ReadAllBytes(@filePath);
				string base64EncodedContent = System.Convert.ToBase64String(contentBytes);
                encodedBinaryFilesNames.Add(filename);
                encodedBinaryFilesContents.Add(base64EncodedContent);
            }
        }
        if (jwlfHeaderName == null) {
            return null;
        }
        string filenameMetadataKey = CsvJwlfConverter.FILENAME_METADATA_KEY;
		jwlfHeaderValuesMap.Add("name", jwlfHeaderName);
        jwlfHeaderValuesMap.Add("metadata", new Dictionary<string, string>() {
				[filenameMetadataKey] = jwlfHeaderName.Replace(".csv", ""),
                [BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY] = "true"
        });

		string? name = null;
		if (jwlfHeaderValuesMap.ContainsKey("name"))
		{
			name = (string)jwlfHeaderValuesMap["name"];
		}
		string? description = null;
		if (jwlfHeaderValuesMap.ContainsKey("description"))
		{
			description = (string)jwlfHeaderValuesMap["description"];
		}
		string? well = null;
		if (jwlfHeaderValuesMap.ContainsKey("well"))
		{
			well = (string)jwlfHeaderValuesMap["well"];
		}
		string? wellbore = null;
		if (jwlfHeaderValuesMap.ContainsKey("wellbore"))
		{
			wellbore = (string)jwlfHeaderValuesMap["wellbore"];
		}
		string? field = null;
		if (jwlfHeaderValuesMap.ContainsKey("field"))
		{
			field = (string)jwlfHeaderValuesMap["field"];
		}
		string? country = null;
		if (jwlfHeaderValuesMap.ContainsKey("country"))
		{
			country = (string)jwlfHeaderValuesMap["country"];
		}
		string? date = null;
		if (jwlfHeaderValuesMap.ContainsKey("date"))
		{
			date = (string)jwlfHeaderValuesMap["date"];
		}
		string? jwlfOperator = null;
		if (jwlfHeaderValuesMap.ContainsKey("operator"))
		{
			jwlfOperator = (string)jwlfHeaderValuesMap["operator"];
		}
		string? serviceCompany = null;
		if (jwlfHeaderValuesMap.ContainsKey("serviceCompany"))
		{
			serviceCompany = (string)jwlfHeaderValuesMap["serviceCompany"];
		}
		string? runNumber = null;
		if (jwlfHeaderValuesMap.ContainsKey("runNumber"))
		{
			runNumber = (string)jwlfHeaderValuesMap["runNumber"];
		}
		double? elevation = null;
		if (jwlfHeaderValuesMap.ContainsKey("elevation"))
		{
			elevation = (double)jwlfHeaderValuesMap["elevation"];
		}
		string? source = null;
		if (jwlfHeaderValuesMap.ContainsKey("source"))
		{
			source = (string)jwlfHeaderValuesMap["source"];
		}
		double? startIndex = null;
		if (jwlfHeaderValuesMap.ContainsKey("startIndex"))
		{
			startIndex = (double)jwlfHeaderValuesMap["startIndex"];
		}
		double? endIndex = null;
		if (jwlfHeaderValuesMap.ContainsKey("endIndex"))
		{
			endIndex = (double)jwlfHeaderValuesMap["endIndex"];
		}
		double? step = null;
		if (jwlfHeaderValuesMap.ContainsKey("step"))
		{
			step = (double)jwlfHeaderValuesMap["step"];
		}
		string? dataUri = null;
		if (jwlfHeaderValuesMap.ContainsKey("dataUri"))
		{
			dataUri = (string)jwlfHeaderValuesMap["dataUri"];
		}
		Dictionary<string, string>metadata = null;
		if (jwlfHeaderValuesMap.ContainsKey("metadata"))
		{
			metadata = ((Dictionary<string, string>)jwlfHeaderValuesMap["metadata"]);
		}

		JwlfHeader jwlfHeader = new JwlfHeader() 
        {
			Name = name,
			Description = description,
			Well = well,
			Wellbore = wellbore,
			Field = field,
			Country = country,
			Date = date,
			JwlfOperator = jwlfOperator,
			ServiceCompany = serviceCompany,
			RunNumber = runNumber,
			Elevation = elevation,
			Source = source,
			StartIndex = startIndex,
			EndIndex = endIndex,
			Step = step,
			DataUri = dataUri,
			Metadata = metadata
		};

        dataEntry.Add(encodedBinaryFilesNames);
        jwlfCurves.Add(new JwlfCurve() 
        {
            Name = BINARIES_NAMES_CURVE_NAME,
            ValueType = JwlfValueType.STRING,
            Dimensions = (ulong) encodedBinaryFilesNames.Count
        });
		jwlfCurves.Add(new JwlfCurve()
		{
			Name = BINARIES_DATA_CURVE_NAME,
			ValueType = JwlfValueType.STRING,
			Dimensions = (ulong)encodedBinaryFilesNames.Count
		});

		dataEntry.Add(encodedBinaryFilesContents);

        List<List<object>> jwlfData = new List<List<object>>();
        jwlfData.Add(dataEntry);
        return new JwlfRequest()
        {
            Header = jwlfHeader,
            Curves = jwlfCurves,
            Data = jwlfData
        };
    }

    public static void ConvertJwlfToFolder(string basePath, JwlfResponse jwlfLog) {
		log.Information("jwlfLog={jwlfLog}", jwlfLog);
        string folderPath = basePath + "/" + jwlfLog.Header.Name + " files";
		try
		{
			Directory.Delete(folderPath, true);
		}
		catch (System.IO.DirectoryNotFoundException e)
		{}

		Directory.CreateDirectory(folderPath);

        int binariesNamesCurveIndex = -1;
        for (int i = 0; i < jwlfLog.Curves.Count; i++) {
            if (BINARIES_NAMES_CURVE_NAME.Equals(jwlfLog.Curves[i].Name)) {
                binariesNamesCurveIndex = i;
            }
        }
        if (binariesNamesCurveIndex == -1) {
            return;
        }

        int binariesDataCurveIndex = -1;
        for (int i = 0; i < jwlfLog.Curves.Count; i++) {
            if (BINARIES_DATA_CURVE_NAME.Equals(jwlfLog.Curves[i].Name)) {
                binariesDataCurveIndex = i;
            }
        }
        if (binariesDataCurveIndex == -1) {
            return;
        }

		List<string> binariesNames = (List<string>)((JsonElement)jwlfLog.Data[0][binariesNamesCurveIndex]).Deserialize(typeof(List<string>));
		jwlfLog.Data[0][binariesNamesCurveIndex] = binariesNames;
		List<string> binariesDataElements = (List<string>)((JsonElement)jwlfLog.Data[0][binariesDataCurveIndex]).Deserialize(typeof(List<string>));
		jwlfLog.Data[0][binariesDataCurveIndex] = binariesDataElements;

		for (int i = 0; i < binariesDataElements.Count; i++) {
			string filePathStr = folderPath + "/" + binariesNames[i];
			string base64EncodedData = binariesDataElements[i];

			byte[] decodedData = System.Convert.FromBase64String(base64EncodedData);
			using (var file = File.Create(filePathStr))
			{
				file.Write(decodedData);
			}
		}
    }

    private static string FindMatch(string text, string regex) {
        var matches = Regex.Matches(text, regex);
        if (matches.Count == 0)
        {
            return null;
        }
		return matches[0].Value;
	}
}
