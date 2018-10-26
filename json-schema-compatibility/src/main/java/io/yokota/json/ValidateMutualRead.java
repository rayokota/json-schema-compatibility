/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.yokota.json;

import io.yokota.json.diff.Difference;
import io.yokota.json.diff.SchemaDiff;
import org.everit.json.schema.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.yokota.json.diff.Difference.Type.*;

/**
 * A {@link SchemaValidationStrategy} that checks that the {@link Schema} to
 * validate and the existing schema can mutually read each other according to
 * the default JSON schema resolution rules.
 */
class ValidateMutualRead implements SchemaValidationStrategy {

    /**
     * Validate that the schemas provided can mutually read data written by each
     * other according to the default JSON schema resolution rules.
     *
     * @throws SchemaValidationException if the schemas are not mutually compatible.
     */
    @Override
    public void validate(Schema toValidate, Schema existing)
        throws SchemaValidationException {
        canRead(toValidate, existing);
        canRead(existing, toValidate);
    }

    /**
     * Validates that data written with one schema can be read using another,
     * based on the default JSON schema resolution rules.
     *
     * @param writtenWith The "writer's" schema, representing data to be read.
     * @param readUsing   The "reader's" schema, representing how the reader will interpret
     *                    data.
     * @throws SchemaValidationException if the schema <b>readUsing<b/> cannot be used to read data
     *                                   written with <b>writtenWith<b/>
     */
    static void canRead(Schema writtenWith, Schema readUsing)
        throws SchemaValidationException {

        final List<Difference> differences = SchemaDiff.findDifferences(writtenWith, readUsing);

        final String errorMessage = differences.stream()
            .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES.contains(diff.getType()))
            .map(ValidateMutualRead::formatErrorMessage)
            .collect(Collectors.joining(", "));
        if (errorMessage != null && errorMessage.length() > 0) {
            throw new SchemaValidationException(readUsing, writtenWith, errorMessage);
        }
    }

    static private String formatErrorMessage(final Difference difference) {
        return difference.getJsonPath() + ": " + difference.getType();
    }
}
