import threading
import time
import uuid
import webbrowser
from urllib.parse import urlencode


class AuthLinkOpener(threading.Thread):
    def __init__(self, appId: str, tenantId: str, local_port: int):
        super().__init__()
        self.appId = appId
        self.tenantId = tenantId
        self.local_port = local_port
        self.state = str(uuid.uuid4())
        self.start()

    def build_query(self):
        url = (
            f"https://login.microsoftonline.com/{self.tenantId}"
            "/oauth2/v2.0/authorize?"
        )
        self.redirect_uri = f"http://localhost:{self.local_port}"
        query = {
            "client_id": self.appId,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
            "response_mode": "query",
            # fixed scope for Azure Databricks
            "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
            "state": self.state,
        }

        return url + urlencode(query)

    def run(self) -> None:
        time.sleep(2)
        webbrowser.open(self.build_query())
