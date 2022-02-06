export:

```
java  \
 -Dexport.jdbc.url=jdbc:postgresql://localhost:5432/alfio \
 -Dexport.jdbc.username=postgres -Dexport.jdbc.password=password \
 -Dexport.table=email_message -Dexport.columns=id,status,recipient \
 -jar dbtojson.jar
```


import:
