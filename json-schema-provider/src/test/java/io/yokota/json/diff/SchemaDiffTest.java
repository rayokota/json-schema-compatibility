package io.yokota.json.diff;

import org.everit.json.schema.JsonSchemaUtil;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.JsonArray;
import org.everit.json.schema.loader.JsonObject;
import org.everit.json.schema.loader.JsonValue;
import org.everit.json.schema.loader.SchemaLoader;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SchemaDiffTest {

    @SuppressWarnings("unchecked")
    @Test
    public void checkJsonSchemaCompatibility() {
        final JsonArray testCases = (JsonArray) JsonValue.of(JsonSchemaUtil.stringToNode(
                readFile("diff-schema-examples.json")));

        for (final Object testCaseObject : testCases.toList()) {
            final JsonObject testCase = (JsonObject) testCaseObject;
            final Schema original = SchemaLoader.load((JsonValue) testCase.get("original_schema"));
            final Schema update = SchemaLoader.load((JsonValue) testCase.get("update_schema"));
            final JsonArray errors = (JsonArray) testCase.get("errors");
            final List<String> errorMessages = (List<String>) errors
                    .toList()
                    .stream()
                    .map(Object::toString)
                    .collect(toList());
            final String description = (String) testCase.get("description");

            assertThat(description, SchemaDiff.compare(original, update).stream()
                    .map(change -> change.getType().toString() + " " + change.getJsonPath())
                    .collect(toList()), is(errorMessages));
        }
    }

    @Test
    public void testRecursiveCheck() {
        final Schema original = SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode(
                readFile("recursive-schema.json"))));
        final Schema newOne = SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode(
                readFile("recursive-schema.json"))));
        Assert.assertTrue(SchemaDiff.compare(original, newOne).isEmpty());
    }

    @Test
    public void testSchemaAddsProperties() {
        final Schema first = SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode("{}")));

        final Schema second = SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode(("{\"properties\": {}}"))));
        final List<Difference> changes = SchemaDiff.compare(first, second);
        Assert.assertTrue(changes.isEmpty());
    }

    public static String readFile(String fileName) {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream is = classLoader.getResourceAsStream(fileName);
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        return null;
    }
}