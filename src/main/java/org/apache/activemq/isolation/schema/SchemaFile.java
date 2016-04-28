package org.apache.activemq.isolation.schema;

import com.google.gson.Gson;
import org.apache.activemq.isolation.schema.json.Message;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SchemaFile {
    public static SchemaFile readFile(String location) throws IOException {
        Path filePath = Paths.get(location);
        String input = StringUtils.join(Files.readAllLines(filePath), "\n");

        Gson gson = new Gson();
        SchemaFile schemaFile = gson.fromJson(input, SchemaFile.class);

        return schemaFile;
    }

    public Message[] definitions;
}
