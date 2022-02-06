package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.support.SqlLobValue;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableImporter {

    private final String tableName;
    private final String insert;
    private final ObjectMapper objectMapper;
    private final NamedParameterJdbcTemplate jdbc;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;

    public TableImporter(
            String tableName,
            List<String> targetColumns,
            Map<String, String> rename,
            ObjectMapper objectMapper,
            NamedParameterJdbcTemplate jdbc,
            TransactionTemplate transactionTemplate,
            int batchSize) {
        this.tableName = tableName;
        this.insert = String.format("insert into %s (%s) values (%s)",
                tableName,
                targetColumns.stream().collect(Collectors.joining(", ")),
                targetColumns.stream().map(c -> ":" + rename.getOrDefault(c, c)).collect(Collectors.joining(", "))
        );
        this.objectMapper = objectMapper;
        this.jdbc = jdbc;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
    }

    public void importTable(BufferedReader br) throws IOException {
        System.out.println("Inserting in table " + tableName);
        String line;
        var params = new ArrayList<MapSqlParameterSource>(batchSize);
        long count = 0;
        while ((line = br.readLine()) != null) {
            var obj = objectMapper.readValue(line, new TypeReference<Map<String, Object>>() {
            });
            var param = new MapSqlParameterSource();
            var metaInfo = (Map<String, Map<String, String>>) obj.get(App.METAINFO);
            for (String column: obj.keySet()) {
                if (App.METAINFO.equals(column)) {
                    continue;
                }
                var columnInfo = metaInfo.get(column);
                addValue(param, column, obj.get(column), columnInfo);
            }
            params.add(param);
            if (params.size() >= batchSize) {
                bulkInsert(params);
                params = new ArrayList<>(batchSize);
            }
            count++;
        }
        if (!params.isEmpty()) {
            bulkInsert(params);
        }
        System.out.println("Inserted " + count + " rows in table " + tableName);
    }

    private void addValue(MapSqlParameterSource param, String name, Object o, Map<String, String> metaInfo) {
        var type = metaInfo.get("type");
        if ("byte[]".equals(type)) {
            param.addValue(name, new SqlLobValue(Base64.getDecoder().decode((String) o)), Types.BLOB);
        } else if ("Timestamp".equals(type)) { // TODO: check about TZ which may be dangerous :D
            param.addValue(name, new Timestamp((long) o));
        } else if ("Time".equals(type)) {
            param.addValue(name, new Time((long) o));
        } else if ("Date".equals(type)) {
            param.addValue(name, new Date((long) o));
        } else {
            param.addValue(name, o);
        }
    }

    private void bulkInsert(List<MapSqlParameterSource> params) {
        transactionTemplate.execute((t) -> {
            jdbc.batchUpdate(insert, params.toArray(MapSqlParameterSource[]::new));
            return null;
        });
    }
}
