/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.yokota.json.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.yokota.json.registry.JsonSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonSerializer;
import org.everit.json.schema.loader.JsonObject;
import org.everit.json.schema.loader.SchemaLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonSchemaConverterTest {

    private final ResourceLoader loader = ResourceLoader.DEFAULT;

    private static final String TOPIC = "topic";

    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private final SchemaRegistryClient schemaRegistry;
    private final JsonSchemaConverter converter;

    public JsonSchemaConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new JsonSchemaConverter(schemaRegistry);
    }

    @Before
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", "http://fake-url");
        config.put("schema.id.field", "/$schemaId");
        converter.configure(config, false);
    }

    @Test
    public void testPrimitive() {
        SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        SchemaAndValue expected = new SchemaAndValue(null, true);
        assertEquals(expected, schemaAndValue);
    }

    @Test
    public void testComplex() throws Exception {
        /*
        JsonObject rawSchemaJson = loader.readObj("/objectschema.json");
        org.everit.json.schema.Schema original = SchemaLoader.load(rawSchemaJson);
        org.everit.json.schema.Schema expected = SchemaLoader.load(rawSchemaJson);
        String subject = TOPIC + "-value";
        NumberSchema jsonSchema = buildWithLocation(NumberSchema.builder());
        schemaRegistry.register(subject, new JsonSchema(jsonSchema));
        */
        SchemaBuilder builder = SchemaBuilder.struct()
            .field("int64", Schema.INT64_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build());
        Schema schema = builder.build();
        Struct original = new Struct(schema)
            .put("int64", 12L)
            .put("float64", 12.2)
            .put("boolean", true)
            .put("string", "foo")
            .put("array", Arrays.asList("a", "b", "c"));
        Map<String, Object> expected = new HashMap<>();
        expected.put("int64", 12L);
        expected.put("float64", 12.2);
        expected.put("boolean", true);
        expected.put("string", "foo");
        expected.put("array", Arrays.asList("a", "b", "c"));
        byte[] converted = converter.fromConnectData(TOPIC, schema, original);
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
        assertEquals(expected, schemaAndValue.value());
    }

    @Test
    public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
        // Version info should be extracted even if the data was not created with Copycat. Manually
        // register a few compatible schemas and validate that data serialized with our normal
        // serializer can be read and gets version info inserted
        String subject = TOPIC + "-value";
        JsonSerializer serializer = new JsonSerializer();
        JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
        jsonConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
        testVersionExtracted(subject, serializer, jsonConverter);

    }

    private void testVersionExtracted(String subject, JsonSerializer serializer, JsonSchemaConverter jsonConverter) throws IOException, RestClientException {
        JsonObject rawSchemaJson1 = loader.readObj("key.json");
        JsonObject rawSchemaJson2 = loader.readObj("keyvalue.json");
        org.everit.json.schema.Schema schema1 = SchemaLoader.load(rawSchemaJson1);
        org.everit.json.schema.Schema schema2 = SchemaLoader.load(rawSchemaJson2);
        schemaRegistry.register(subject, new JsonSchema(schema1));
        schemaRegistry.register(subject, new JsonSchema(schema2));

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode objectNode1 = mapper.createObjectNode();
        objectNode1.put("$schemaId", 1);
        objectNode1.put("key", 15);

        ObjectNode objectNode2 = mapper.createObjectNode();
        objectNode2.put("$schemaId", 2);
        objectNode2.put("key", 15);
        objectNode2.put("value", "bar");

        // Get serialized data
        byte[] serializedRecord1 = serializer.serialize(TOPIC, objectNode1);
        byte[] serializedRecord2 = serializer.serialize(TOPIC, objectNode2);

        SchemaAndValue converted1 = jsonConverter.toConnectData(TOPIC, serializedRecord1);
        assertEquals(1L, (long) converted1.schema().version());

        SchemaAndValue converted2 = jsonConverter.toConnectData(TOPIC, serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());
    }

    @Test
    public void testSameSchemaMultipleTopicForValue() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
        jsonConverter.configure(SR_CONFIG, false);
        assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, false);
    }

    @Test
    public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
        jsonConverter.configure(SR_CONFIG, true);
        assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, true);
    }

    private void assertSameSchemaMultipleTopic(JsonSchemaConverter converter, SchemaRegistryClient schemaRegistry, boolean isKey) throws IOException, RestClientException {
        JsonObject rawSchemaJson1 = loader.readObj("key.json");
        JsonObject rawSchemaJson2_1 = loader.readObj("keyvalue.json");
        JsonObject rawSchemaJson2_2 = loader.readObj("keyvalue.json");
        org.everit.json.schema.Schema schema1 = SchemaLoader.load(rawSchemaJson1);
        org.everit.json.schema.Schema schema2_1 = SchemaLoader.load(rawSchemaJson2_1);
        org.everit.json.schema.Schema schema2_2 = SchemaLoader.load(rawSchemaJson2_2);
        String subjectSuffix = isKey ? "key" : "value";
        int schema2_1_id = schemaRegistry.register("topic1-" + subjectSuffix, new JsonSchema(schema2_1));
        int schema1_id = schemaRegistry.register("topic2-" + subjectSuffix, new JsonSchema(schema1));
        int schema2_2_id = schemaRegistry.register("topic2-" + subjectSuffix, new JsonSchema(schema2_2));

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode objectNode1 = mapper.createObjectNode();
        objectNode1.put("$schemaId", schema2_1_id);
        objectNode1.put("key", 15);
        objectNode1.put("value", "bar");

        ObjectNode objectNode2 = mapper.createObjectNode();
        objectNode2.put("$schemaId", schema2_2_id);
        objectNode2.put("key", 15);
        objectNode2.put("value", "bar");

        JsonSerializer serializer = new JsonSerializer();
        byte[] serializedRecord1 = serializer.serialize("topic1", objectNode1);
        byte[] serializedRecord2 = serializer.serialize("topic2", objectNode2);

        SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
        assertEquals(1L, (long) converted1.schema().version());

        SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());

        converted2 = converter.toConnectData("topic2", serializedRecord2);
        assertEquals(2L, (long) converted2.schema().version());
    }

    public static <S extends org.everit.json.schema.Schema> S buildWithLocation(
        org.everit.json.schema.Schema.Builder<S> builder) {
        return builder.schemaLocation("#").build();
    }

}
