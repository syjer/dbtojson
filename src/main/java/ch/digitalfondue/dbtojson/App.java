package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ch.digitalfondue.dbtojson.DbToJson.exportData;
import static ch.digitalfondue.dbtojson.DbToJson.importData;

/**
 *
 */
public class App {

    final static String METAINFO = "__metainfo";

    public static void main(String[] args) throws IOException {

        var mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        var exportJdbcUrl = System.getProperty("export.jdbc.url");
        var exportJdbcUsername = System.getProperty("export.jdbc.username");
        var exportJdbcPassword = System.getProperty("export.jdbc.password");
        var exportTable = System.getProperty("export.table");
        var exportColumns = System.getProperty("export.columns");

        var exportBulk = System.getProperty("export.bulk");
        var importBulk = System.getProperty("import.bulk");

        var importJdbcUrl = System.getProperty("import.jdbc.url");
        var importTable = System.getProperty("import.table");
        var importColumns = System.getProperty("import.columns");
        var importFile = System.getProperty("import.file");

        if (exportJdbcUrl != null && exportTable != null && exportColumns != null) {
            exportData(mapper, exportJdbcUrl, exportJdbcUsername, exportJdbcPassword, exportTable, fromCSV(exportColumns), System.getProperty("export.file"));
        } else if (exportBulk != null) {
            var conf = mapper.readValue(new FileInputStream(exportBulk), BulkOp.class);
            for (var tableInfo : conf.getTables()) {
                exportData(mapper, conf.getJdbcUrl(), conf.getUsername(), conf.getPassword(), tableInfo.getName(), tableInfo.getColumns(), tableInfo.getName());
            }
        } else if (importJdbcUrl != null && importTable != null && importColumns != null && importFile != null) {
            var columns = fromCSV(importColumns);
            importData(mapper, importJdbcUrl, System.getProperty("import.jdbc.username"), System.getProperty("import.jdbc.password"), importTable, columns, importFile);
        } else if (importBulk != null) {
            var conf = mapper.readValue(new FileInputStream(importBulk), BulkOp.class);
            for (var tableInfo : conf.getTables()) {
                importData(mapper, conf.getJdbcUrl(), conf.getUsername(), conf.getPassword(), tableInfo.getName(), tableInfo.getColumns(), tableInfo.getFile());
            }
        } else {
            System.err.println("Missing parameters, was not able to decide which mode (import/export) was requested");
        }
    }

    private static List<String> fromCSV(String value) {
        return Stream.of(value.split(",")).map(s -> s.trim()).collect(Collectors.toList());
    }

}
