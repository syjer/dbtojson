package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BulkExport {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final List<TableInfo> tables;

    @JsonCreator
    public BulkExport(@JsonProperty("jdbcUrl") String jdbcUrl,
                      @JsonProperty("username") String username,
                      @JsonProperty("password") String password,
                      @JsonProperty("tables") List<TableInfo> tables) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tables = tables;
    }

    public static class TableInfo {
        private final String name;
        private final List<String> columns;

        @JsonCreator
        public TableInfo(@JsonProperty("name") String name, @JsonProperty("columns") List<String> columns) {
            this.name = name;
            this.columns = columns;
        }

        public String getName() {
            return name;
        }

        public List<String> getColumns() {
            return columns;
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public List<TableInfo> getTables() {
        return tables;
    }
}
