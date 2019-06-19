# ipython-cosmos-extension
IPython Extension for Cosmos SQL

## Install the extension:
```bash
!pip install  ipython-cosmos-extension
```
## Load the extension:
  This extension assumes cosmosdb endpoint credentials
  are set as environment variables accessible by
  ``COSMOS_ENDPOINT`` and ``COSMOS_KEY``.
```bash
%load_ext cosmos_sql
```
## Set Database name
```bash
%database <your_database_name>
```
## Set Container name     
```bash
%container <your_container_name>
``` 
## Execute Cosmos SQL Statements
```bash
%%sql
select * from user
```
or you can specify different database or collection

```bash
%%sql --database testdb --container testcol2
SELECT top 1 r.id, r._ts from r order by r._ts desc

```

To get the result from the command use ``_`` variable. 
## Auto conversion of result to data frame
   Disabling auto conversion of result to dataframe:
```bash
%disable_autoconvert_to_dataframe 
```
   Enabling auto conversion of result to dataframe:
```bash
%enable_autoconvert_to_dataframe 
```


# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
