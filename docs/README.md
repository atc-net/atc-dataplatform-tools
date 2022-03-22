
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
$> atc_dp_tools_update_req_file requirements.txt
```

will output the freeze version for all libraries that are installed directly,
or as a dependency of the libraries specified in `requirements.txt`.
The recommended usecase is to output this list of libarries to a freeze file
such as 

```
$> atc_dp_tools_update_req_file requirements.txt > requirements_freeze.txt
```

and then use the freeze file when installing your product. You can always 
update your libraries by rerunning this last command.