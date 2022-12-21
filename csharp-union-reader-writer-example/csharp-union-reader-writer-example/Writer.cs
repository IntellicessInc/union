using Serilog.Core;
using System.Net.Http.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Union;

public class Writer 
{

    private static Logger log = AppLogFactory.CreateLogger();


    public void Main()
    {
        Dictionary<string, JwlfValueType> headerValueTypeMapping = new Dictionary<string, JwlfValueType>
        {
            { "Integer Number 1", JwlfValueType.INTEGER },
            { "Float Number 2", JwlfValueType.FLOAT},
            { "String Text A", JwlfValueType.STRING}
        };
        IAppConfiguration config;
        try
        {
            config = EnvAppConfiguration.Init();
        }
        catch(Exception e)
        {
            log.Warning(e, "Using writer dev configuration because of the following failure in initialization of environment variables based configuration:");
            config = DevAppConfiguration.InitWriter();
        }
        string writerLocalWorkingDirectory = config.GetLocalWorkingDirectory();
        var writerDirectory = new DirectoryInfo(@writerLocalWorkingDirectory);
        if(!writerDirectory.Exists)
        {
            throw new Exception("Writer local working directory with path='" + writerLocalWorkingDirectory + "' is not directory");
        }

        UnionClient unionClient = new UnionClient(
            new UnionClientConfiguration
            {
                UnionUrl=config.GetUnionUrl(),
                AuthProviderUrl=config.GetAuthProviderUrl(),
                AuthProviderRealm=config.GetAuthProviderRealm(),
                AuthClientId=config.GetAuthClientId(),
                Username=config.GetAuthUserUsername(),
                Password=config.GetAuthUserPassword()
            }
        );

        string client = config.GetUnionClient();
        string region = config.GetUnionRegion();
        string asset = config.GetUnionAsset();
        string folder = config.GetUnionFolder();
        
        log.Information("Listening to local writer folder...");
        while (true)
        {
            FileInfo[] fileInfos = writerDirectory.GetFiles();
            if (fileInfos.Length > 0)
            {
                foreach (FileInfo fileInfo in fileInfos)
                {
                    string filePath = fileInfo.FullName;
                    string filename = fileInfo.Name;
                    if (filename.EndsWith(".csv"))
                    {
                        string csvContent = File.ReadAllText(@filePath);
                        JwlfRequest jwlfRequest = CsvJwlfConverter.ConvertCsvToJwlf(asset, filename, csvContent, headerValueTypeMapping);
                        string filenameWithoutExtension = filename.Replace(".csv", "");
                        jwlfRequest.Header.Metadata["filename"] = filenameWithoutExtension;

                        JwlfSavedResponse savedJwlf = unionClient.SaveJwlfLog(client, region, asset, folder, jwlfRequest);
                        log.Information("JWLF Log from file '{Filename}' got saved with id={SavedJwlfId}", filename, savedJwlf.Id);
                    }
                    else if (filename.EndsWith(".json"))
                    {
                        string jsonContent = File.ReadAllText(@filePath);
                        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
                        options.Converters.Add(new JwlfValueTypeJsonConverter());
                        List<JwlfRequest> jwlfRequests = JsonSerializer.Deserialize<List<JwlfRequest>>(jsonContent, options);
                        string filenameWithoutExtension = filename.Replace(".json", "");
                        for(int i = 0; i < jwlfRequests.Count; i++)
                        {
                            JwlfRequest jwlfRequest = jwlfRequests[i];
                            jwlfRequest.Header.Metadata["filename"] = filenameWithoutExtension + (i + 1);
                        }
                        JwlfSavedResponseList savedJwlfs = unionClient.SaveJwlfLogs(client, region, asset, folder, jwlfRequests);
                        log.Information("JWLF Logs from file '{Filename}' got saved with ids={SavedJwlfLogsIds}", filename, savedJwlfs.List.Select(jwlf => jwlf.Id));
}
                    fileInfo.Delete();
                }
            }
            Thread.Sleep(1000);
        }
    }
}
