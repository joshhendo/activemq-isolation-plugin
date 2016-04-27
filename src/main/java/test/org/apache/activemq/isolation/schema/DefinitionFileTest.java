package test.org.apache.activemq.isolation.schema;

import com.google.gson.Gson;
import org.apache.activemq.isolation.schema.SchemaFile;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class DefinitionFileTest {
    @Test
    public void read() throws Exception {
        SchemaFile schemaFile = SchemaFile.readFile("./assets/definition.json");
    }
}
