package ch.digitalfondue.dbtojson;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableImporter {

    private final String insert;

    public TableImporter(String tableName, List<String> targetColumns, Map<String, String> rename) {
        this.insert = String.format("insert into %s (%s) values (%s)",
                tableName,
                targetColumns.stream().collect(Collectors.joining(", ")),
                targetColumns.stream().map(c -> ":" + rename.getOrDefault(c, c)).collect(Collectors.joining(", "))
        );
    }

    public void importTable(BufferedReader br) throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            // TODO: read X line, then commit the batch
        }
    }
}
