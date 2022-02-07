A Table to jsonl.gz exporter/importer
===
A small utility for 
 - exporting a given table from a jdbc connection to a jsonl output (or file, compressed).
 - importing a file to a given table with the given jdbc connection.


All the code is under the Apache License Version 2.0.

Any contribution welcome :).

Currently you must build it yourself as it's not 100% clear if we can distribute the full jar
with the drivers included.

Currently the following drivers are included:

 - mariadb
 - postgresql
 - oracle

## Use

### Via descriptor


#### Export:

Create a export.json descriptor:

```json
{
  "jdbcUrl": "jdbc:myjdbcurl",
  "username": "username",
  "password": "pwd",
  "tables": [
    {
      "name": "table1",
      "columns": ["col1", "col2"]
    },
    {
      "name": "table2",
      "columns": ["col1", "col2"]
    }
  ]
}
```

And then export with

> java -Dexport.bulk=export.json -jar dbtojson.jar

#### Import:


Create a import.json descriptor (you can copy the export one and modify it):

```json
{
  "jdbcUrl": "jdbc:myjdbcurl",
  "username": "username",
  "password": "pwd",
  "tables": [
    {
      "name": "table1",
      "columns": ["col1", "col2"],
      "file": "table1.jsonl.gz"
    },
    {
      "name": "table2",
      "columns": ["col1", "col2"],
      "file": "table2.jsonl.gz"
    }
  ]
}
```

And then export with

> java -Dimport.bulk=export.json -jar dbtojson.jar

### Via Params


#### export:


```
java  \
 -Dexport.jdbc.url=jdbc:postgresql://localhost:5432/alfio \
 -Dexport.jdbc.username=postgres -Dexport.jdbc.password=password \
 -Dexport.table=email_message -Dexport.columns=id,status,recipient \
 -Dexport.file=file
 -jar dbtojson.jar
```


#### import:


```
java  \
 -Dimport.jdbc.url=jdbc:postgresql://localhost:5432/alfio \
 -Dimport.jdbc.username=postgres -import.jdbc.password=password \
 -Dimport.table=email_message -import.columns=id,status,recipient \
 -Dimport.file=file.jsonl.gz
 -jar dbtojson.jar
```