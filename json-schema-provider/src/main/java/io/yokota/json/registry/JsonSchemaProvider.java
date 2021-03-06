/**
 * Copyright 2018 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.SchemaProvider;

public class JsonSchemaProvider implements SchemaProvider {

    @Override
    public String schemaType() {
        return JsonSchema.JSON;
    }

    @Override
    public ParsedSchema parseSchema(String schemaString) {
        try {
            return new JsonSchema(schemaString);
        } catch (Exception e) {
            return null;
        }
    }
}
