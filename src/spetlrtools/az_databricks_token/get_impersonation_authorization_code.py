import sys
from types import SimpleNamespace

from spetlrtools.az_databricks_token.AuthLinkOpener import AuthLinkOpener
from spetlrtools.az_databricks_token.server import LocalServer


def get_impersonation_authorization_code(appId: str, tenantId: str):
    # to get the callback, launch a server on localhost
    webServer = LocalServer()
    print("Opening browser, please log in...", file=sys.stderr)

    # open the webbrowser in another thread after two seconds.
    opener = AuthLinkOpener(
        appId=appId, tenantId=tenantId, local_port=webServer.server_port
    )

    # handle one request (blocking)
    webServer.handle_request()

    try:
        result = webServer.last_query
    except AttributeError:
        raise Exception("No callback was received in time. Please re-try.")

    if result.state != opener.state:
        raise Exception("Invalid state field in result.")

    return SimpleNamespace(
        auth_code=result.code, state=opener.state, redirect_uri=opener.redirect_uri
    )
