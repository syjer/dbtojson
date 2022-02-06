package ch.digitalfondue.dbtojson;


import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AppTest {

    //@Test
    public void printJson() throws IOException {
        System.setProperty("export.jdbc.url", "jdbc:postgresql://localhost:5432/alfio");
        System.setProperty("export.jdbc.username", "postgres");
        System.setProperty("export.jdbc.password", "password");
        System.setProperty("export.table", "email_message");
        System.setProperty("export.columns", "id,status,recipient,request_ts");
        App.main(new String[] {});
    }

    //@Test
    public void exportJsonWithBlob() throws IOException {
        System.setProperty("export.jdbc.url", "jdbc:postgresql://localhost:5432/alfio");
        System.setProperty("export.jdbc.username", "postgres");
        System.setProperty("export.jdbc.password", "password");
        System.setProperty("export.table", "file_blob");
        System.setProperty("export.columns", "id,name,content_size,content,content_type,creation_time,attributes");
        System.setProperty("export.file", "file_blob");
        App.main(new String[] {});
    }

    @Test
    public void importJsonWithBlob() throws IOException {
        System.setProperty("import.jdbc.url", "jdbc:postgresql://localhost:5432/alfio");
        System.setProperty("import.jdbc.username", "postgres");
        System.setProperty("import.jdbc.password", "password");
        System.setProperty("import.table", "file_blob_2");
        System.setProperty("import.columns", "id,name,content_size,content,content_type,creation_time,attributes");
        System.setProperty("import.file", "file_blob.jsonl.gz");
        App.main(new String[] {});
    }
}
