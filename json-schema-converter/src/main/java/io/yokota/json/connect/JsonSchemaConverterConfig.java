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

package io.yokota.json.connect;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import java.util.Map;

public class JsonSchemaConverterConfig extends AbstractKafkaSchemaSerDeConfig {

    public static final String SCHEMA_ID_FIELD_CONFIG = "schema.id.field";
    public static final String SCHEMA_ID_FIELD_DEFAULT = "/$schemaId";
    private static final String SCHEMA_ID_FIELD_DOC = "JsonPointer path to schema ID field in JSON document";

    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = baseConfigDef();
        CONFIG
            .define(
                SCHEMA_ID_FIELD_CONFIG,
                Type.STRING,
                SCHEMA_ID_FIELD_DEFAULT,
                Importance.HIGH,
                SCHEMA_ID_FIELD_DOC
            )
            .define(
                SCHEMAS_CACHE_SIZE_CONFIG,
                Type.INT,
                SCHEMAS_CACHE_SIZE_DEFAULT,
                Importance.HIGH,
                SCHEMAS_CACHE_SIZE_DOC
            );
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public JsonSchemaConverterConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public String schemaIdField() {
        return getString(SCHEMA_ID_FIELD_CONFIG);
    }

    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }
}
