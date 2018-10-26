/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.yokota.json.connect;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.yokota.json.registry.JsonSchema;
import io.yokota.json.registry.JsonSchemaProvider;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.storage.Converter;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of Converter that supports JSON with JSON Schema.
 */
public class JsonSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {

    private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> value.booleanValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> (byte) value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> (short) value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> value.intValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> value.longValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> value.floatValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> value.doubleValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value) -> {
            try {
                return value.binaryValue();
            } catch (IOException e) {
                throw new DataException("Invalid bytes field", e);
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, (schema, value) -> {
            Schema elemSchema = schema == null ? null : schema.valueSchema();
            ArrayList<Object> result = new ArrayList<>();
            for (JsonNode elem : value) {
                result.add(convertToConnect(elemSchema, elem));
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, (schema, value) -> {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();

            // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
            // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
            // schema, we default to encoding in a Map.
            Map<Object, Object> result = new HashMap<>();
            if (schema == null || keySchema.type() == Schema.Type.STRING) {
                if (!value.isObject())
                    throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                while (fieldIt.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fieldIt.next();
                    result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
                }
            } else {
                if (!value.isArray())
                    throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                for (JsonNode entry : value) {
                    if (!entry.isArray())
                        throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                    if (entry.size() != 2)
                        throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                    result.put(convertToConnect(keySchema, entry.get(0)),
                        convertToConnect(valueSchema, entry.get(1)));
                }
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, (schema, value) -> {
            if (!value.isObject())
                throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

            // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
            // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
            // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
            // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
            // just returns the schema Object and has no overhead.
            Struct result = new Struct(schema.schema());
            for (Field field : schema.fields())
                result.put(field, convertToConnect(field.schema(), value.get(field.name())));

            return result;
        });
    }

    // Convert values in Kafka Connect form into their logical types. These logical converters are discovered by logical type
    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();

    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof byte[]))
                throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
            return Decimal.toLogical(schema, (byte[]) value);
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof Integer))
                throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
            return Date.toLogical(schema, (int) value);
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof Integer))
                throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
            return Time.toLogical(schema, (int) value);
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
            return Timestamp.toLogical(schema, (long) value);
        });
    }

    private static final HashMap<String, LogicalTypeConverter> TO_JSON_LOGICAL_CONVERTERS = new HashMap<>();

    static {
        TO_JSON_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof BigDecimal))
                throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
            return Decimal.fromLogical(schema, (BigDecimal) value);
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
            return Date.fromLogical(schema, (java.util.Date) value);
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
            return Time.fromLogical(schema, (java.util.Date) value);
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
            return Timestamp.fromLogical(schema, (java.util.Date) value);
        });
    }


    private JsonPointer schemaIdPointer = JsonPointer.compile(JsonSchemaConverterConfig.SCHEMA_ID_FIELD_DEFAULT);
    private int cacheSize = JsonSchemaConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT;
    private Cache<ParsedSchemaWithVersion, Schema> toConnectSchemaCache;
    private boolean isKey;
    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();

    public JsonSchemaConverter() {
    }

    // Public only for testing
    public JsonSchemaConverter(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public ConfigDef config() {
        return JsonSchemaConverterConfig.configDef();
    }

    public void configure(Map<String, ?> configs) {
        JsonSchemaConverterConfig config = new JsonSchemaConverterConfig(configs);
        configureClientProperties(config, new JsonSchemaProvider());

        schemaIdPointer = JsonPointer.compile(config.schemaIdField());
        cacheSize = config.schemaCacheSize();
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<>(cacheSize));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);

        Map<String, Object> conf = new HashMap<>(configs);
        configure(conf);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        JsonNode jsonValue = convertToJson(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (jsonValue == null)
            return SchemaAndValue.NULL;

        Integer id = getSchemaId(jsonValue);
        Schema schema = null;
        try {
            if (id != null) {
                String subject = getSubjectName(topic, isKey, null, null);
                ParsedSchema parsedSchema = schemaRegistry.getBySubjectAndId(subject, id);
                Integer version = schemaRegistry.getVersion(subject, parsedSchema);
                schema = asConnectSchema(parsedSchema, version);
            }
            return new SchemaAndValue(schema, convertToConnect(schema, jsonValue));
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Error deserializing JSON message for id " + id, e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving JSON schema for id " + id, e);
        }
    }

    private Integer getSchemaId(JsonNode value) throws DataException {
        JsonNode idNode = value.at(schemaIdPointer);
        if (idNode.isInt()) {
            return idNode.intValue();
        } else {
            return null;
        }
    }

    public Schema asConnectSchema(ParsedSchema jsonSchema) {
        return asConnectSchema(jsonSchema, null, true, null);
    }

    public Schema asConnectSchema(ParsedSchema jsonSchema, Integer version) {
        return asConnectSchema(jsonSchema, null, true, version);
    }

    public Schema asConnectSchema(ParsedSchema schema, String fieldName, Boolean required, Integer version) {
        if (schema == null)
            return null;

        if (version != null) {
            Schema cached = toConnectSchemaCache.get(new ParsedSchemaWithVersion(schema, version));
            if (cached != null) {
                return cached;
            }
        }

        org.everit.json.schema.Schema jsonSchema = ((JsonSchema) schema).schemaObj;

        final SchemaBuilder builder;
        if (jsonSchema instanceof BooleanSchema) {
            builder = SchemaBuilder.bool();
        } else if (jsonSchema instanceof NumberSchema) {
            NumberSchema numberSchema = (NumberSchema) jsonSchema;
            builder = numberSchema.requiresInteger() ? SchemaBuilder.int64() : SchemaBuilder.float64();
        } else if (jsonSchema instanceof StringSchema) {
            builder = SchemaBuilder.string();
        } else if (jsonSchema instanceof EnumSchema) {
            // TODO enum symbol preservation
            builder = SchemaBuilder.string();
        } else if (jsonSchema instanceof CombinedSchema) {
            // TODO use struct
            builder = SchemaBuilder.struct();
        } else if (jsonSchema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) jsonSchema;
            org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();

            if (itemsSchema == null) {
                throw new DataException(fieldName + " array schema did not specify the items type");
            }

            builder = SchemaBuilder.array(asConnectSchema(new JsonSchema(itemsSchema)));
        } else if (jsonSchema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
            builder = SchemaBuilder.struct();
            Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();
            List<String> requiredFields = objectSchema.getRequiredProperties();

            for (Map.Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
                String subFieldName = property.getKey();
                org.everit.json.schema.Schema subSchema = property.getValue();
                Boolean subFieldRequired = requiredFields.contains(subFieldName);
                builder.field(subFieldName,
                    asConnectSchema(new JsonSchema(subSchema), subFieldName, subFieldRequired, null));
            }
        } else if (jsonSchema instanceof ReferenceSchema) {
            ReferenceSchema refSchema = (ReferenceSchema) jsonSchema;
            return asConnectSchema(new JsonSchema(refSchema.getReferredSchema()), fieldName, required, version);
        } else {
            throw new DataException("Unsupported schema type " + jsonSchema.getClass().getName());
        }

        if (fieldName != null) {
            builder.name(fieldName);
        }

        if (version != null) {
            builder.version(version);
        }

        if (required) {
            builder.required();
        } else {
            builder.optional();
        }

        String description = jsonSchema.getDescription();
        if (description != null) {
            builder.doc(description);
        }

        if (jsonSchema.hasDefaultValue()) {
            builder.defaultValue(jsonSchema.getDefaultValue());
        }

        Schema result = builder.build();
        if (version != null) {
            toConnectSchemaCache.put(new ParsedSchemaWithVersion(new JsonSchema(jsonSchema), version), result);
        }
        return result;
    }


    /**
     * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private static JsonNode convertToJson(Schema schema, Object logicalValue) {
        if (logicalValue == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JsonNodeFactory.instance.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        Object value = logicalValue;
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_JSON_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                value = logicalConverter.convert(schema, logicalValue);
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode((Byte) value);
                case INT16:
                    return JsonNodeFactory.instance.numberNode((Short) value);
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) value);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) value);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) value);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) value);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JsonNodeFactory.instance.textNode(charSeq.toString());
                case BYTES:
                    if (value instanceof byte[])
                        return JsonNodeFactory.instance.binaryNode((byte[]) value);
                    else if (value instanceof ByteBuffer)
                        return JsonNodeFactory.instance.binaryNode(((ByteBuffer) value).array());
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection collection = (Collection) value;
                    ArrayNode list = JsonNodeFactory.instance.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode fieldValue = convertToJson(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING;
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode)
                        obj = JsonNodeFactory.instance.objectNode();
                    else
                        list = JsonNodeFactory.instance.arrayNode();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode mapKey = convertToJson(keySchema, entry.getKey());
                        JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

                        if (objectMode)
                            obj.set(mapKey.asText(), mapValue);
                        else
                            list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    ObjectNode obj = JsonNodeFactory.instance.objectNode();
                    for (Field field : schema.fields()) {
                        obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
                    }
                    return obj;
                }
            }

            throw new DataException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }


    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue.isNull()) {
                if (schema.defaultValue() != null)
                    return schema.defaultValue(); // any logical type conversions should already have been applied
                if (schema.isOptional())
                    return null;
                throw new DataException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            switch (jsonValue.getNodeType()) {
                case NULL:
                    // Special case. With no schema
                    return null;
                case BOOLEAN:
                    schemaType = Schema.Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber())
                        schemaType = Schema.Type.INT64;
                    else
                        schemaType = Schema.Type.FLOAT64;
                    break;
                case ARRAY:
                    schemaType = Schema.Type.ARRAY;
                    break;
                case OBJECT:
                    schemaType = Schema.Type.MAP;
                    break;
                case STRING:
                    schemaType = Schema.Type.STRING;
                    break;

                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
                    break;
            }
        }

        final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + String.valueOf(schemaType));

        Object converted = typeConverter.convert(schema, jsonValue);
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                converted = logicalConverter.convert(schema, converted);
        }
        return converted;
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    private interface LogicalTypeConverter {
        Object convert(Schema schema, Object value);
    }

    private static class ParsedSchemaWithVersion {
        private ParsedSchema schema;
        private Integer version;

        public ParsedSchemaWithVersion(ParsedSchema schema, Integer version) {
            this.schema = schema;
            this.version = version;
        }

        public ParsedSchema schema() {
            return schema;
        }

        public Integer version() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParsedSchemaWithVersion that = (ParsedSchemaWithVersion) o;
            return Objects.equals(schema, that.schema) &&
                Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schema, version);
        }
    }
}
