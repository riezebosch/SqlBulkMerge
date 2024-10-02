# sql2yaml

> Import &amp; export sql tables into yaml files and vice versa.
 

## Export

```shell
dotnet tool sql2yaml export -c 'Data Source=.;Initial Catalog=.;...' -d ./export -t table1 table2 table3
```

## Import

```shell
dotnet tool sql2yaml import -c 'Data Source=.;Initial Catalog=.;...' -d ./export
```

The import uses a temporary table for staging and merges the data into the target table.

All triggers and foreign keys are disabled before execution and re-enabled again when done.

The entire import is executed within a transaction.