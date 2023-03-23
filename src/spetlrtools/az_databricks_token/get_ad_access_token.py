import sys
from json import JSONDecodeError
from pprint import pprint

import requests


def get_ad_access_token(
    appId: str,
    tenantId: str,
    auth_code: str,
    state: str,
    redirect_uri: str,
    verbose: bool = False,
):
    if verbose:
        print("  get_ad_access_token:", file=sys.stderr)
        print(f"     appId:        {appId[:7]}...", file=sys.stderr)
        print(f"     tenantId:     {tenantId[:7]}...", file=sys.stderr)
        print(f"     auth_code:    {auth_code[:7]}...", file=sys.stderr)
        print(f"     state:        {state[:7]}...", file=sys.stderr)
        print(f"     redirect_uri: {redirect_uri}", file=sys.stderr)

    url = f"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token"
    data = dict(
        client_id=appId,
        scope="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
        code=auth_code,
        redirect_uri=redirect_uri,
        grant_type="authorization_code",
        state=state,
    )
    r = requests.post(url=url, data=data)

    try:
        result = r.json()
    except JSONDecodeError:
        raise Exception(f"Unexpected result from login.microsoftonline.com:\n{r.text}")

    if "access_token" not in result:
        if verbose:
            print("Error received:", file=sys.stderr)
            pprint(result, stream=sys.stderr)

        if "error_codes" in result:
            raise Exception(f"Login Exception code {result['error_codes'].pop()}")
        else:
            raise Exception("Unknown Login Exception")

    return result["access_token"]
