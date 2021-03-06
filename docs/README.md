
# Requirements manipulation

It is good style to keep your dependencies fixed at a certain
version to prevent dependency updates from breaking your product
and to prevent so-called digital supply chain attacks. A good way of 
doing this is to specify the exact version of all dependencies in a 
`requirements.txt`, for example:

```
atc-dataplatform==0.5.2
```

At the same time it is good to run at the latest patched version of all 
dependencies. The `requirements.txt` file therefore needs to be updated 
regularly with all the latest versions. `atc-dataplatform-tools` provides
a command line tool for doing this:

```
$> atc_dp_freeze_req requirements.txt
```

will output the freeze version for all libraries that are installed directly,
or as a dependency of the libraries specified in `requirements.txt`.
The recommended use-case is to output this list of libraries to a freeze file
such as 

```
$> atc_dp_freeze_req requirements.txt -o requirements_freeze.txt
```

and then use the freeze file when installing your product. You can always 
update your libraries by re-running this last command.

Sometimes you may prefer to keep your dependencies in `setup.cfg`.
The tool covers this case as well. We recommend that you maintain a separate
file with requirements. The frozen version of all (sub-)dependencies can then 
be injected into your configuration file with

```
$> atc_dp_freeze_req requirements.txt --cfg
```

The help message of the tool is shown here for completeness:
```
usage: atc_dp_freeze_req [-h] [-o, --out-file OUT_FILE] [--cfg] [--cfg-file CFG_FILE] [--reject REJECT] in_file

Update requirement versions in specified file.

positional arguments:
  in_file               The requirements file to read.

optional arguments:
  -h, --help            show this help message and exit
  -o, --out-file OUT_FILE
                        The output to file.
  --cfg
  --cfg-file CFG_FILE   specify your configuration file if it differs from setup.cfg
  --reject REJECT       regex to exclude. Default: pip|pywin32
```

# Azure Databricks AD Token

If you want to use azure AD tokens to access the Databricks API
(instead of the personal access tokens that you can pull from the
web frontend), you can follow 
[this guide here](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/app-aad-token).
Set the redirect URI to `localhost` exactly as in the example.

After setting up the initial web-app for authentication, you can use 
the command line tool provided by this package to get the token quickly.

```
$> atc_az_databricks_token --appId $appId --tenantId $tenantId --workspaceUrl $workspaceUrl
```

The parameters `appId` and `tenantId` correspond to the web-app that you registered.
If no further parameters are given the databricks token will be printed to
the console for use in your deployment pipeline.

If you set the optional parameter `workspaceUrl`, the tool will instead 
overwrite your `~/.databrickscfg` file with the provided workspace url
and with the newly generated token.