package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
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
        } else if (o instanceof Long) {
            return "Long";
        } else if (o instanceof byte[]) {
            return "byte[]";
        } else if (o instanceof Timestamp) {
            return "Timestamp";
        } else if (o instanceof Time) {
            return "Time";
        } else if (o instanceof Date) {
            return "Date";
        } else if (o instanceof BigDecimal) {
            return "BigDecimal";
        } else if (o instanceof BigInteger) {
            return "BigInteger";
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
                if (obj instanceof Clob) {
                    var clob = (Clob) obj;
                    try (var r = clob.getCharacterStream()) {
                        obj = IOUtils.toString(r);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
                if (obj instanceof Blob) {
                    var blob = (Blob) obj;
                    try (var is = blob.getBinaryStream()) {
                        obj = IOUtils.toByteArray(is);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
                metaInfo.put(columnName, Map.of("type", extractType(obj)));
                res.put(columnName, obj);
            }
            res.put(App.METAINFO, metaInfo);
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
