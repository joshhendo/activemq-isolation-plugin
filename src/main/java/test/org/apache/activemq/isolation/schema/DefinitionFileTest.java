package test.org.apache.activemq.isolation.schema;

import com.google.gson.Gson;
import com.sun.deploy.util.StringUtils;
import org.apache.activemq.isolation.schema.SchemaFile;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class DefinitionFileTest {
    @Test
    public void read() throws Exception {
        Path filePath = Paths.get("./assets/definition.json");
        String input = StringUtils.join(Files.readAllLines(filePath), "\n");

        Gson gson = new Gson();
        SchemaFile schemaFile = gson.fromJson(input, SchemaFile.class);

    }
}
