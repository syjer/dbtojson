A Table to jsonl.gz exporter/importer
===
A small utility for 
 - exporting a given table from a jdbc connection to a jsonl output (or file, compressed).
 - importing a file to a given table with the given jdbc connection.


All the code is under the Apache License Version 2.0.

## Use

export:

```
java  \
 -Dexport.jdbc.url=jdbc:postgresql://localhost:5432/alfio \
 -Dexport.jdbc.username=postgres -Dexport.jdbc.password=password \
 -Dexport.table=email_message -Dexport.columns=id,status,recipient \
 -Dexport.file=file
 -jar dbtojson.jar
```


import:

```
java  \
 -Dimport.jdbc.url=jdbc:postgresql://localhost:5432/alfio \
 -Dimport.jdbc.username=postgres -import.jdbc.password=password \
 -Dimport.table=email_message -import.columns=id,status,recipient \
 -Dimport.file=file.jsonl.gz
 -jar dbtojson.jar
```