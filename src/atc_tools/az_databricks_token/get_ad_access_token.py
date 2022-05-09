import requests


def get_ad_access_token(
    appId: str, tenantId: str, auth_code: str, state: str, redirect_uri: str
):
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
    return (r.json())["access_token"]
