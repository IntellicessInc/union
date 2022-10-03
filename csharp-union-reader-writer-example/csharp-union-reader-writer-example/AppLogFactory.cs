using Serilog;
using Serilog.Core;

namespace Union;

public static class AppLogFactory
{

    private static string LOGGING_LEVEL_ENV = "LOGGING_LEVEL";
    private static LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

    private static LogLevel? _logLevel = null;

    public static void Init()
    {
        
         _logLevel = DEFAULT_LOG_LEVEL;
        try
        {
            string loggingLevelString = Environment.GetEnvironmentVariable(LOGGING_LEVEL_ENV);
            if (loggingLevelString != null)
            {
                loggingLevelString = loggingLevelString.ToUpperInvariant();
                LogLevel logLevel = (LogLevel)Enum.Parse(typeof(LogLevel), loggingLevelString);
                _logLevel = logLevel;
            }
        }
        catch(Exception e)
        {
            CreateLogger().Warning(e, "Error occured while initiating log level based on environment variable called {LoggingLevelEnv}", LOGGING_LEVEL_ENV);
        }
    }

    public static Logger CreateLogger()
    {
        if (!_logLevel.HasValue)
        { 
            Init();
        }
        var config = new LoggerConfiguration();

        if (LogLevel.DEBUG == _logLevel.Value)
        {
            config = config.MinimumLevel.Debug();
        } 
        else if (LogLevel.INFO == _logLevel.Value)
        {
            config = config.MinimumLevel.Information();
        }
        else if (LogLevel.WARN == _logLevel.Value)
        {
            config = config.MinimumLevel.Warning();
        }
        else if (LogLevel.ERROR == _logLevel.Value)
        {
            config = config.MinimumLevel.Error();
        }

        return config
            .WriteTo.Console()
            .CreateLogger();
    }
}
