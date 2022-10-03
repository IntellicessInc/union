using System;
using System.Collections.Generic;

namespace Union;

public class DevAppConfiguration : IAppConfiguration
{
    private Type _type;

    private DevAppConfiguration(Type type)
    {
        _type = type;
    }

    public static DevAppConfiguration InitReader()
    {
        return new DevAppConfiguration(Type.READER);
    }

    public static DevAppConfiguration InitWriter()
    {
        return new DevAppConfiguration(Type.WRITER);
    }

    public string GetAuthProviderUrl()
    {
        return "https://dev-intellicess-keycloak.southcentralus.cloudapp.azure.com";
    }

    public string GetAuthProviderRealm()
    {
        return "public-test";
    }

    public string GetAuthClientId()
    {
        return "public-client";
    }

    public string GetAuthUserUsername()
    {
        if (_type == Type.READER)
        {
            return "reader";
        }
        else if (_type == Type.WRITER)
        {
            return "writer";
        }
        throw new Exception("Type='" + _type + "' and is not recognized");
    }

    public string GetAuthUserPassword()
    {
        if (_type == Type.READER)
        {
            return "reader-password";
        }
        else if (_type == Type.WRITER)
        {
            return "writer-password";
        }
        throw new Exception("Type='" + _type + "' and is not recognized");
    }

    public string GetUnionUrl()
    {
        // return "https://dev-dsp.southcentralus.cloudapp.azure.com";
        return "http://localhost:8080";
    }

    public string GetUnionClient()
    {
        return "public-test";
    }

    public string GetUnionRegion()
    {
        return "texas";
    }

    public string GetUnionAsset()
    {
        return "well1";
    }

    public string GetUnionFolder()
    {
        return "shared-data";
    }

    public string GetLocalWorkingDirectory()
    {
        if (_type == Type.READER)
        {
            return "../../../../reader-folder";
        }
        else if (_type == Type.WRITER)
        {
            return "../../../../writer-folder";
        }
        throw new Exception("Type='" + _type + "' and is not recognized");
    }

    private enum Type
    {
        READER, WRITER
    }
}
