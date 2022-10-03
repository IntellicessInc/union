using System;
using System.Collections.Generic;

namespace Union;

public interface IAppConfiguration
{
    string GetAuthProviderUrl();

    string GetAuthProviderRealm();

    string GetAuthClientId();

    string GetAuthUserUsername();

    string GetAuthUserPassword();

    string GetUnionUrl();

    string GetUnionClient();

    string GetUnionRegion();

    string GetUnionAsset();

    string GetUnionFolder();

    string GetLocalWorkingDirectory();
}
