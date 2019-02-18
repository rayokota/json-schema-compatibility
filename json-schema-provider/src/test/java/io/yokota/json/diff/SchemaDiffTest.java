package io.yokota.json.diff;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SchemaDiffTest {

    @SuppressWarnings("unchecked")
    @Test
    public void checkJsonSchemaCompatibility() {
        final JSONArray testCases = new JSONArray(
                readFile("diff-schema-examples.json"));

        for (final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final Schema original = SchemaLoader.load(testCase.getJSONObject("original_schema"));
            final Schema update = SchemaLoader.load(testCase.getJSONObject("update_schema"));
            final JSONArray errors = (JSONArray) testCase.get("errors");
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
        final Schema original = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        final Schema newOne = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        Assert.assertTrue(SchemaDiff.compare(original, newOne).isEmpty());
    }

    @Test
    public void testSchemaAddsProperties() {
        final Schema first = SchemaLoader.load(new JSONObject("{}"));

        final Schema second = SchemaLoader.load(new JSONObject(("{\"properties\": {}}")));
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