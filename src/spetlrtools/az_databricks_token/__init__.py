"""
This package implements the MSAL flow to obtain a personal api token for an instance
of azure databricks without using the web interface.

see url:
https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/app-aad-token

see also:
https://docs.microsoft.com/en-us/azure/active-directory/develop/reply-url#localhost-exceptions

Required input:
- applicationId (for the auth application)
- tenantId


"""

# from spetlrtools.az_databricks_token.main import main
# from spetlrtools.az_databricks_token.server import LocalRequestHandler
#
