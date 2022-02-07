package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class BulkOp {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final List<TableInfo> tables;

    @JsonCreator
    public BulkOp(@JsonProperty("jdbcUrl") String jdbcUrl,
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
        private final String file;

        @JsonCreator
        public TableInfo(@JsonProperty("name") String name,
                         @JsonProperty("columns") List<String> columns,
                         @JsonProperty("file") String file) {
            this.name = name;
            this.columns = columns;
            this.file = file;
        }

        public String getName() {
            return name;
        }

        public List<String> getColumns() {
            return columns;
        }

        public String getFile() {
            return file;
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
