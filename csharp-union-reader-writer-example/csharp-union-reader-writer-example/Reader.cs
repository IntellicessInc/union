using Serilog.Core;
using System.Net.Http.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Newtonsoft.Json;
using Union;

public class Reader 
{

    private static Logger log = AppLogFactory.CreateLogger();


    public static void Main() 
    {
        IAppConfiguration config;
        try
        {
            config = EnvAppConfiguration.Init();
        }
        catch(Exception e)
        {
            log.Warning(e, "Using reader dev configuration because of the following failure in initialization of environment variables based configuration:");
            config = DevAppConfiguration.InitReader();
        }
        string readerLocalWorkingDirectory = config.GetLocalWorkingDirectory();
        var readerDirectory = new DirectoryInfo(readerLocalWorkingDirectory);
        if(!readerDirectory.Exists)
        {
            throw new Exception("Reader local working directory with path='" + readerLocalWorkingDirectory + "' is not directory");
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

        // if the following variable is set to null, it will read all data that was saved so far in given data space (i.e. in client/region/asset/folder)
        long inclusiveSinceTimestamp = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - 5000;
        
        log.Information("Listening to '{client}/{region}/{asset}/{folder}' data space in Union since timestamp='{inclusiveSinceTimestamp}'...",
            client, region, asset, folder, inclusiveSinceTimestamp);
        foreach(var jwlfStreamEvent in unionClient.GetJwlfsStream(client, region, asset, folder, inclusiveSinceTimestamp))
        {
            JwlfResponse jwlfLog = jwlfStreamEvent.Data.Content;
            string filename = jwlfLog.Header.Metadata["filename"];
            string base64EncodedBinariesKey = CsvJwlfWithBase64EncodedBinariesConverter.BASE64_ENCODED_BINARIES_EXAMPLE_METADATA_KEY;
            bool base64EncodedBinariesExample = jwlfLog.Header.Metadata.ContainsKey(base64EncodedBinariesKey)
                && bool.Parse(jwlfLog.Header.Metadata[base64EncodedBinariesKey]);
            if (base64EncodedBinariesExample) {
                CsvJwlfWithBase64EncodedBinariesConverter.ConvertJwlfToFolder(readerLocalWorkingDirectory, jwlfLog);
            }

            if (!filename.EndsWith(".json")) {
                filename += ".json";
            }
            string filepath = readerLocalWorkingDirectory + "/" + filename;
            SaveAsPrettyJson(jwlfLog, filepath);
            log.Information("Pulled '{filename}' and saved in local reader folder", filename);
        }
    }

    private static void SaveAsPrettyJson(JwlfResponse jwlfResponse, string filepath)
    {
        using (var file = File.CreateText(filepath))
        {
            string json = JsonConvert.SerializeObject(jwlfResponse, Formatting.Indented);
            file.Write(json);   
        }
    }
}
