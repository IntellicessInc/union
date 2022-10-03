namespace Union;

public class EnvAppConfiguration : IAppConfiguration
{

    private static string AUTH_PROVIDER_URL_ENV = "AUTH_PROVIDER_URL";
    private static string AUTH_PROVIDER_REALM_ENV = "AUTH_PROVIDER_REALM";
    private static string AUTH_CLIENT_ID_ENV = "AUTH_CLIENT_ID";
    private static string AUTH_USER_USERNAME_ENV = "AUTH_USER_USERNAME";
    private static string AUTH_USER_PASSWORD_ENV = "AUTH_USER_PASSWORD";
    private static string UNION_URL_ENV = "UNION_URL";
    private static string UNION_CLIENT_ENV = "UNION_CLIENT";
    private static string UNION_REGION_ENV = "UNION_REGION";
    private static string UNION_ASSET_ENV = "UNION_ASSET";
    private static string UNION_FOLDER_ENV = "UNION_FOLDER";
    private static string LOCAL_WORKING_DIRECTORY_ENV = "LOCAL_WORKING_DIRECTORY";

    private string _authProviderUrl;
    private string _authProviderRealm;
    private string _authClientId;
    private string _authUserUsername;
    private string _authUserPassword;
    private string _unionUrl;
    private string _unionClient;
    private string _unionRegion;
    private string _unionAsset;
    private string _unionFolder;
    private string _localWorkingDirectory;

    private EnvAppConfiguration()
    {
        _authProviderUrl = RequireEnvVar(AUTH_PROVIDER_URL_ENV);
        _authProviderRealm = RequireEnvVar(AUTH_PROVIDER_REALM_ENV);
        _authClientId = RequireEnvVar(AUTH_CLIENT_ID_ENV);
        _authUserUsername = RequireEnvVar(AUTH_USER_USERNAME_ENV);
        _authUserPassword = RequireEnvVar(AUTH_USER_PASSWORD_ENV);
        _unionUrl = RequireEnvVar(UNION_URL_ENV);
        _unionClient = RequireEnvVar(UNION_CLIENT_ENV);
        _unionRegion = RequireEnvVar(UNION_REGION_ENV);
        _unionAsset = RequireEnvVar(UNION_ASSET_ENV);
        _unionFolder = RequireEnvVar(UNION_FOLDER_ENV);
        _localWorkingDirectory = RequireEnvVar(LOCAL_WORKING_DIRECTORY_ENV);
    }

    public static EnvAppConfiguration Init()
    {
        return new EnvAppConfiguration();
    }

    public string GetAuthProviderUrl()
    {
        return _authProviderUrl;
    }

    public string GetAuthProviderRealm()
    {
        return _authProviderRealm;
    }

    public string GetAuthClientId()
    {
        return _authClientId;
    }

    public string GetAuthUserUsername()
    {
        return _authUserUsername;
    }

    public string GetAuthUserPassword()
    {
        return _authUserPassword;
    }

    public string GetUnionUrl()
    {
        return _unionUrl;
    }

    public string GetUnionClient()
    {
        return _unionClient;
    }

    public string GetUnionRegion()
    {
        return _unionRegion;
    }

    public string GetUnionAsset()
    {
        return _unionAsset;
    }

    public string GetUnionFolder()
    {
        return _unionFolder;
    }

    public string GetLocalWorkingDirectory()
    {
        return _localWorkingDirectory;
    }

    private static string RequireEnvVar(string name)
    {
        string variable = System.Environment.GetEnvironmentVariable(name);
        if (variable == null)
        {
            throw new Exception("Environment variable with name '" + name + "' is required!");
        }
        return variable;
    }
}
