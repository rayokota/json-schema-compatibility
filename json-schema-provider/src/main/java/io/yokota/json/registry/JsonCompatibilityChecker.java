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

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.yokota.json.SchemaValidationException;
import io.yokota.json.SchemaValidator;
import io.yokota.json.SchemaValidatorBuilder;
import org.everit.json.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JsonCompatibilityChecker {

    // Check if the new schema can be used to read data produced by the previous schema
    private static final SchemaValidator BACKWARD_VALIDATOR =
        new SchemaValidatorBuilder().canReadStrategy().validateLatest();
    public static final JsonCompatibilityChecker BACKWARD_CHECKER
        = new JsonCompatibilityChecker(BACKWARD_VALIDATOR);

    // Check if data produced by the new schema can be read by the previous schema
    private static final SchemaValidator FORWARD_VALIDATOR =
        new SchemaValidatorBuilder().canBeReadStrategy().validateLatest();
    public static final JsonCompatibilityChecker FORWARD_CHECKER
        = new JsonCompatibilityChecker(FORWARD_VALIDATOR);

    // Check if the new schema is both forward and backward compatible with the previous schema
    private static final SchemaValidator FULL_VALIDATOR =
        new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();
    public static final JsonCompatibilityChecker FULL_CHECKER
        = new JsonCompatibilityChecker(FULL_VALIDATOR);

    // Check if the new schema can be used to read data produced by all earlier schemas
    private static final SchemaValidator BACKWARD_TRANSITIVE_VALIDATOR =
        new SchemaValidatorBuilder().canReadStrategy().validateAll();
    public static final JsonCompatibilityChecker BACKWARD_TRANSITIVE_CHECKER
        = new JsonCompatibilityChecker(BACKWARD_TRANSITIVE_VALIDATOR);

    // Check if data produced by the new schema can be read by all earlier schemas
    private static final SchemaValidator FORWARD_TRANSITIVE_VALIDATOR =
        new SchemaValidatorBuilder().canBeReadStrategy().validateAll();
    public static final JsonCompatibilityChecker FORWARD_TRANSITIVE_CHECKER
        = new JsonCompatibilityChecker(FORWARD_TRANSITIVE_VALIDATOR);

    // Check if the new schema is both forward and backward compatible with all earlier schemas
    private static final SchemaValidator FULL_TRANSITIVE_VALIDATOR =
        new SchemaValidatorBuilder().mutualReadStrategy().validateAll();
    public static final JsonCompatibilityChecker FULL_TRANSITIVE_CHECKER
        = new JsonCompatibilityChecker(FULL_TRANSITIVE_VALIDATOR);

    private static final SchemaValidator NO_OP_VALIDATOR = (schema, schemas) -> {
        // do nothing
    };
    public static final JsonCompatibilityChecker NO_OP_CHECKER = new JsonCompatibilityChecker(
        NO_OP_VALIDATOR);

    private final SchemaValidator validator;

    private JsonCompatibilityChecker(SchemaValidator validator) {
        this.validator = validator;
    }

    // visible for testing
    public boolean isCompatible(Schema newSchema, List<Schema> previousSchemas) {
        List<Schema> previousSchemasCopy = new ArrayList<>(previousSchemas);
        try {
            // Validator checks in list order, but checks should occur in reverse chronological order
            Collections.reverse(previousSchemasCopy);
            validator.validate(newSchema, previousSchemasCopy);
        } catch (SchemaValidationException e) {
            return false;
        }

        return true;
    }

    public static JsonCompatibilityChecker checker(CompatibilityLevel level) {
        switch (level) {
            case NONE:
                return JsonCompatibilityChecker.NO_OP_CHECKER;
            case BACKWARD:
                return JsonCompatibilityChecker.BACKWARD_CHECKER;
            case BACKWARD_TRANSITIVE:
                return JsonCompatibilityChecker.BACKWARD_TRANSITIVE_CHECKER;
            case FORWARD:
                return JsonCompatibilityChecker.FORWARD_CHECKER;
            case FORWARD_TRANSITIVE:
                return JsonCompatibilityChecker.FORWARD_TRANSITIVE_CHECKER;
            case FULL:
                return JsonCompatibilityChecker.FULL_CHECKER;
            case FULL_TRANSITIVE:
                return JsonCompatibilityChecker.FULL_TRANSITIVE_CHECKER;
            default:
                throw new IllegalArgumentException("Invalid level " + level);
        }
    }
}
