import argparse
from pathlib import Path

from spetlrtools.az_databricks_token.get_ad_access_token import get_ad_access_token
from spetlrtools.az_databricks_token.get_impersonation_authorization_code import (
    get_impersonation_authorization_code,
)


def main():
    parser = argparse.ArgumentParser(
        description="Request Azure Databricks MSAL token.",
        epilog="For more information see https://docs.microsoft.com"
        "/en-us/azure/databricks/dev-tools/api/latest/aad/app-aad-token",
    )
    parser.add_argument("--appId", required=True, help="the authorizing webapp")
    parser.add_argument("--tenantId", required=True, help="the tenant of the webapp")
    parser.add_argument(
        "--workspaceUrl", help="directly set .databrickscfg if this is given"
    )
    # help="debug printing"
    parser.add_argument("-v", "--verbose", action="store_true", help=argparse.SUPPRESS)

    args = parser.parse_args()

    # 1. Request an authorization code, which launches a browser window and asks
    # for Azure user login. The authorization code is returned after the user
    # successfully logs in.
    results = get_impersonation_authorization_code(
        appId=args.appId, tenantId=args.tenantId
    )

    # 2. Use the authorization code to acquire the Azure AD access token.
    access_token = get_ad_access_token(
        appId=args.appId,
        tenantId=args.tenantId,
        auth_code=results.auth_code,
        state=results.state,
        redirect_uri=results.redirect_uri,
        verbose=args.verbose,
    )

    if args.workspaceUrl:
        dbcfg = (
            "[DEFAULT]\n"
            f"host = https://{args.workspaceUrl}\n"
            f"token = {access_token}"
        )
        with open(Path.home() / ".databrickscfg", "w") as f:
            f.write(dbcfg)
        print("Your local .databrickscfg has been updated with a token.")
    else:
        print(access_token)
