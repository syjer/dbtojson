package ch.digitalfondue.dbtojson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
public class App {
    public static void main(String[] args) throws IOException {

        var mapper = new ObjectMapper();
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        var exportJdbcUrl = System.getProperty("export.jdbc.url");
        var exportTable = System.getProperty("export.table");
        var exportColumns = System.getProperty("export.columns");
        if (exportJdbcUrl != null && exportTable != null && exportColumns != null) {
            var dsExport = new HikariDataSource();
            dsExport.setJdbcUrl(exportJdbcUrl);
            dsExport.setUsername(System.getProperty("export.jdbc.username"));
            dsExport.setPassword(System.getProperty("export.jdbc.password"));
            var columns = Stream.of(exportColumns.split(",")).map(s -> s.trim()).collect(Collectors.toList());
            System.out.println(String.format("Exporting table %s with columns %s", exportTable, exportColumns));
            var jdbcTemplate = new NamedParameterJdbcTemplate(dsExport);
            var exporter = new TableExporter(exportTable, columns, jdbcTemplate, mapper);
            var file = System.getProperty("export.file");
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
}
