package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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

        var importJdbcUrl = System.getProperty("import.jdbc.url");
        var importTable = System.getProperty("import.table");
        var importColumns = System.getProperty("import.columns");
        var importFile = System.getProperty("import.file");

        if (exportJdbcUrl != null && exportTable != null && exportColumns != null) {
            exportData(mapper, exportJdbcUrl, exportJdbcUsername, exportJdbcPassword, exportTable, fromCSV(exportColumns), System.getProperty("export.file"));
        } else if (exportBulk != null) {
            var conf = mapper.readValue(new FileInputStream(exportBulk), BulkExport.class);
            for (var tableInfo : conf.getTables()) {
                exportData(mapper, conf.getJdbcUrl(), conf.getUsername(), conf.getPassword(), tableInfo.getName(), tableInfo.getColumns(), tableInfo.getName());
            }
        } else if (importJdbcUrl != null && importTable != null && importColumns != null && importFile != null) {
            importData(mapper, importJdbcUrl, importTable, importColumns, importFile);
        } else {
            System.err.println("Missing parameters, was not able to decide which mode (import/export) was requested");
        }
    }

    private static List<String> fromCSV(String value) {
        return Stream.of(value.split(",")).map(s -> s.trim()).collect(Collectors.toList());
    }

    private static void importData(ObjectMapper mapper, String importJdbcUrl, String importTable, String importColumns, String importFile) throws IOException {
        var dsImport = new HikariDataSource();
        dsImport.setJdbcUrl(importJdbcUrl);
        dsImport.setUsername(System.getProperty("import.jdbc.username"));
        dsImport.setPassword(System.getProperty("import.jdbc.password"));
        var columns = fromCSV(importColumns);
        var jdbcTemplate = new NamedParameterJdbcTemplate(dsImport);
        var tm = new DataSourceTransactionManager(dsImport);
        var importer = new TableImporter(importTable, columns, Map.of(), mapper, jdbcTemplate, new TransactionTemplate(tm), 50);
        try (var r = fromFile(importFile)) {
            importer.importTable(r);
        }
    }


    public static boolean isGZipped(File f) {
        try (var raf = new RandomAccessFile(f, "r")){
            int magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
            return magic == GZIPInputStream.GZIP_MAGIC;
        } catch (Throwable e) {
            return false;
        }
    }

    private static BufferedReader fromFile(String file) throws IOException {
        var f = new File(file);
        InputStream i = new FileInputStream(f);
        if (isGZipped(f)) {
            i = new GZIPInputStream(i);
        }
        return new BufferedReader(new InputStreamReader(i, StandardCharsets.UTF_8));
    }

    private static void exportData(ObjectMapper mapper,
                                   String exportJdbcUrl,
                                   String username,
                                   String password,
                                   String exportTable,
                                   List<String> columns,
                                   String file) throws IOException {
        var dsExport = new HikariDataSource();
        dsExport.setJdbcUrl(exportJdbcUrl);
        dsExport.setUsername(username);
        dsExport.setPassword(password);
        System.out.println(String.format("Exporting table %s with columns %s", exportTable, columns.stream().collect(Collectors.joining(", "))));
        var jdbcTemplate = new NamedParameterJdbcTemplate(dsExport);
        var exporter = new TableExporter(exportTable, columns, jdbcTemplate, mapper);
        PrintWriter os;
        if (file != null) {
            var f = new File(file + ".jsonl.gz");
            System.out.println("Output to file " + f.getAbsoluteFile());
            os = new PrintWriter(new GZIPOutputStream(new FileOutputStream(f)), false, StandardCharsets.UTF_8);
        } else {
            os = new PrintWriter(System.out);
        }
        exporter.exportTable(os);
        os.flush();
        if (file != null) {
            os.close();
        }
    }
}
