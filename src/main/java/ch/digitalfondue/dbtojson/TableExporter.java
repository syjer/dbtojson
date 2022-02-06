package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TableExporter {
    private final List<String> columnNames;
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final String query;

    public TableExporter(String tableName,
                         List<String> columnNames,
                         NamedParameterJdbcTemplate jdbc,
                         ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.columnNames = columnNames;
        this.query = String.format("select %s from %s",
                columnNames.stream().collect(Collectors.joining(", ")),
                tableName);
    }

    private static String extractType(Object o) {
        if (o instanceof String) {
            return "String";
        } else if (o instanceof Integer) {
            return "Integer";
        } else if (o instanceof Double) {
            return "Double";
        } else if (o instanceof Boolean) {
            return "Boolean";
        } else if (o instanceof byte[]) {
            return "byte[]";
        } else if (o instanceof Timestamp) {
            return "Timestamp";
        }
        return "unknown";
    }

    public void exportTable(PrintWriter os) {
        var counter = new AtomicLong();
        jdbc.query(query, rs -> {
            var res = new LinkedHashMap<String, Object>();
            // handle types
            var metaInfo = new LinkedHashMap<String, Map<String, String>>();
            for (var columnName: columnNames) {
                var obj = rs.getObject(columnName);
                metaInfo.put(columnName, Map.of("type", extractType(obj)));
                res.put(columnName, obj);
            }
            res.put("__metainfo", metaInfo);
            try {
                objectMapper.writeValue(os, res);
                os.flush();
                os.println();
                os.flush();
                counter.incrementAndGet();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        System.out.println("Exported " + counter.get() + " rows");
    }
}
