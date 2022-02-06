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
    public void printJsonWithBlob() throws IOException {
        System.setProperty("export.jdbc.url", "jdbc:postgresql://localhost:5432/alfio");
        System.setProperty("export.jdbc.username", "postgres");
        System.setProperty("export.jdbc.password", "password");
        System.setProperty("export.table", "file_blob");
        System.setProperty("export.columns", "id,name,content_size,content,content_type,creation_time,attributes");
        System.setProperty("export.file", "file_blob");
        App.main(new String[] {});
    }
}
