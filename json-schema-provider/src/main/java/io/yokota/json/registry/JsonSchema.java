/**
 * Copyright 2014 Confluent Inc.
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
 */

package io.yokota.json.registry;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.yokota.json.diff.Difference;
import io.yokota.json.diff.SchemaDiff;
import org.everit.json.schema.JsonSchemaUtil;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.JsonValue;
import org.everit.json.schema.loader.SchemaLoader;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JsonSchema implements ParsedSchema {

    public final static String JSON = "JSON";

    public final Schema schemaObj;

    private final Integer version;

    public JsonSchema(Schema schemaObj) {
        this(schemaObj, null);
    }

    public JsonSchema(Schema schemaObj, Integer version) {
        this.schemaObj = schemaObj;
        this.version = version;
    }

    public JsonSchema(String schemaString) {
        this(SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode(schemaString))));
    }

    public JsonSchema(String schemaString, Integer version) {
        this(SchemaLoader.load(JsonValue.of(JsonSchemaUtil.stringToNode(schemaString))), version);
    }

    @Override
    public String schemaType() {
        return JSON;
    }

    @Override
    public String canonicalString() {
        return schemaObj.toString();
    }


    @Override
    public Integer version() {
        return version;
    }

    @Override
    public boolean isBackwardCompatible(ParsedSchema previousSchema) {
        if (!schemaType().equals(previousSchema.schemaType())) {
            return false;
        }
        final List<Difference> differences = SchemaDiff.compare(((JsonSchema) previousSchema).schemaObj, schemaObj);

        final String errorMessage = differences.stream()
            .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
            .map(JsonSchema::formatErrorMessage)
            .collect(Collectors.joining(", "));
        return errorMessage == null || errorMessage.length() <= 0;
    }

    static private String formatErrorMessage(final Difference difference) {
        return difference.getJsonPath() + ": " + difference.getType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonSchema that = (JsonSchema) o;
        return Objects.equals(schemaObj, that.schemaObj) &&
            Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaObj, version);
    }

    @Override
    public String toString() {
        return schemaObj.toString();
    }
}
