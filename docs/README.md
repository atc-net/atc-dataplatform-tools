
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

# Test Job submission

The command `atc_test_job` can be used to run unit-test on a databricks cluster.
The command can submit a single job run, but additionally:

 - your unittests folder is archived and sent to databricks so that tests can be run 
   on a cluster
 - a main script is automatically pushed to databricks, so you don't have to supply 
   your own
 - At a specified level below your total tests-folder, the job is split into parallel 
   tasks
 - Test output is collected by using the cluster log functionality of databricks
 - a fetch command can return all stdout

## How to submit

Usage:
```powershell
usage: atc-test-job submit [-h] [--wheels WHEELS] --tests TESTS (--task TASK | --tasks-from TASKS_FROM) (--cluster CLUSTER | --cluster-file CLUSTER_FILE)
                           [--sparklibs SPARKLIBS | --sparklibs-file SPARKLIBS_FILE] [--requirement REQUIREMENT | --requirements-file REQUIREMENTS_FILE] [--main-script MAIN_SCRIPT]    
                           [--pytest-args PYTEST_ARGS] [--out-json OUT_JSON]

Run Test Cases on databricks cluster.

optional arguments:
  -h, --help            show this help message and exit
  --wheels WHEELS       The glob paths of all wheels under test.
  --tests TESTS         Location of the tests folder. Will be sendt to databricks as a whole.
  --task TASK           Single Test file or folder to execute.
  --tasks-from TASKS_FROM
                        path in test archive where each subfolder becomes a task.
  --cluster CLUSTER     JSON document describing the cluster setup.
  --cluster-file CLUSTER_FILE
                        File with JSON document describing the cluster setup.
  --sparklibs SPARKLIBS
                        JSON document describing the spark dependencies.
  --sparklibs-file SPARKLIBS_FILE
                        File with JSON document describing the spark dependencies.
  --requirement REQUIREMENT
                        a python dependency, specified like for pip
  --requirements-file REQUIREMENTS_FILE
                        File with python dependencies, specified like for pip
  --main-script MAIN_SCRIPT
                        Your own test_main.py script file, to add custom functionality.
  --pytest-args PYTEST_ARGS
                        Additional arguments to pass to pytest in each test job.
  --out-json OUT_JSON   File to store the RunID for future queries.
```

```powershell
atc-test-job submit `
    --tests tests `
    --tasks-from tests/cluster/job4 `
    --cluster-file cluster.json `
    --requirements-file requirements.txt `
    --sparklibs-file sparklibs.json `
    --out-json test.json
```

- `tests/` should be the folder containing all your tests. In the test run, you will 
  be able to reference it from the local folder `import tests.my.tool`
- `cluster/job4` will be the part of the test library from which tests will be 
  executed. In this example, the folder `tests/cluster/job4` exists. Its sub-folders 
  will be executed in one task per subfolder inside the test job.
- `cluster.json` should contain a cluster description for a job. Example
```json
{
  "spark_version": "9.1.x-scala2.12",
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*, 4]",
    "spark.databricks.delta.preview.enabled": true,
    "spark.databricks.io.cache.enabled":true
  },
  "azure_attributes": {
    "availability": "ON_DEMAND_AZURE",
    "first_on_demand": 1,
    "spot_bid_max_price": -1
  },
  "node_type_id": "Standard_DS3_v2",
  "custom_tags": {
    "ResourceClass":"SingleNode"
  },
  "spark_env_vars": {
    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
  },
  "num_workers": 0
}
```
- the optional `requirements.txt` should contain a pip-style list of requirements
- the optional `sparklibs.json` should contain spark dependencies as an array. Example:
```json
[
    {
        "maven": {
            "coordinates": "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0"
        }
    }
]
```
- optionally, the run ID is written to `test.json` so that it does not have to be 
  provided on the command line when fetching.

## How to fetch
Usage:
```powershell
usage: atc-test-job fetch [-h] (--runid RUNID | --runid-json RUNID_JSON) [--stdout STDOUT] [--failfast]

Return test run result.

optional arguments:
  -h, --help            show this help message and exit
  --runid RUNID         Run ID of the test job
  --runid-json RUNID_JSON
                        File with JSON document describing the Run ID of the test job.
  --stdout STDOUT       Output test stdout to this file.
  --failfast            Stop and cancel job on first failed task.
```

The `fetch` operation consists of the following steps:
- periodically query the job progress and print updates to the console.
- if any task completes, the stdout file is downloaded
- if `failfast` is selected, a single failed task will result in a cancelling of the 
  overall job.
- If the job succeeds, the command will return with 0 return value, making it 
  suitable for use in test pipelines.

Example fetch:
```powershell
atc-test-job fetch --runid-json .\test.json --stdout .\stdout.txt
```

- The run ID can be supplied through a file or directly in the command line.
- if `stdout` is set, the output will be written to this file instead of printing to 
  the console.