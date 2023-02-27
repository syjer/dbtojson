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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DbToJson {


    public ObjectMapper getMapper() {
        var mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        return mapper;
    }

    public void export(BulkOp conf) throws IOException {
        var mapper = getMapper();
        for (var tableInfo : conf.getTables()) {
            exportData(mapper, conf.getJdbcUrl(), conf.getUsername(), conf.getPassword(), tableInfo.getName(), tableInfo.getColumns(), tableInfo.getName());
        }
    }

    static void exportData(ObjectMapper mapper,
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

    public void importData(BulkOp conf) throws IOException {
        var mapper = getMapper();
        for (var tableInfo : conf.getTables()) {
            importData(mapper, conf.getJdbcUrl(), conf.getUsername(), conf.getPassword(), tableInfo.getName(), tableInfo.getColumns(), tableInfo.getFile());
        }
    }

    public static void importData(ObjectMapper mapper, String importJdbcUrl, String username, String password, String importTable, List<String> columns, String importFile) throws IOException {
        var dsImport = new HikariDataSource();
        dsImport.setJdbcUrl(importJdbcUrl);
        dsImport.setUsername(username);
        dsImport.setPassword(password);
        var jdbcTemplate = new NamedParameterJdbcTemplate(dsImport);
        var tm = new DataSourceTransactionManager(dsImport);
        var importer = new TableImporter(importTable, columns, Map.of(), mapper, jdbcTemplate, new TransactionTemplate(tm), 50);
        try (var r = fromFile(importFile)) {
            importer.importTable(r);
        }
    }

    public static boolean isGZipped(File f) {
        try (var raf = new RandomAccessFile(f, "r")) {
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
}
