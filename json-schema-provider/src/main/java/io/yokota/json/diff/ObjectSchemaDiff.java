package io.yokota.json.diff;

import org.everit.json.schema.ObjectSchema;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static io.yokota.json.diff.Difference.Type.*;

public class ObjectSchemaDiff {
    static void compare(final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        compareRequired(ctx, original, update);
        compareProperties(ctx, original, update);
        compareDependencies(ctx, original, update);
        compareAdditionalProperties(ctx, original, update);
        compareAttributes(ctx, original, update);
    }

    private static void compareAttributes(
        final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        if (!Objects.equals(original.getMaxProperties(), update.getMaxProperties())) {
            if (original.getMaxProperties() == null && update.getMaxProperties() != null) {
                ctx.addDifference("maxProperties", MAX_PROPERTIES_ADDED);
            } else if (original.getMaxProperties() != null && update.getMaxProperties() == null) {
                ctx.addDifference("maxProperties", MAX_PROPERTIES_REMOVED);
            } else if (original.getMaxProperties() < update.getMaxProperties()) {
                ctx.addDifference("maxProperties", MAX_PROPERTIES_INCREASED);
            } else if (original.getMaxProperties() > update.getMaxProperties()) {
                ctx.addDifference("maxProperties", MAX_PROPERTIES_DECREASED);
            }
        }
        if (!Objects.equals(original.getMinProperties(), update.getMinProperties())) {
            if (original.getMinProperties() == null && update.getMinProperties() != null) {
                ctx.addDifference("minProperties", MIN_PROPERTIES_ADDED);
            } else if (original.getMinProperties() != null && update.getMinProperties() == null) {
                ctx.addDifference("minProperties", MIN_PROPERTIES_REMOVED);
            } else if (original.getMinProperties() < update.getMinProperties()) {
                ctx.addDifference("minProperties", MIN_PROPERTIES_INCREASED);
            } else if (original.getMinProperties() > update.getMinProperties()) {
                ctx.addDifference("minProperties", MIN_PROPERTIES_DECREASED);
            }
        }
    }

    private static void compareAdditionalProperties(
        final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        try (Context.PathScope pathScope = ctx.enterPath("additionalProperties")) {
            if (original.permitsAdditionalProperties() != update.permitsAdditionalProperties()) {
                if (update.permitsAdditionalProperties()) {
                    ctx.addDifference(ADDITIONAL_PROPERTIES_ADDED);
                } else {
                    ctx.addDifference(ADDITIONAL_PROPERTIES_REMOVED);
                }
            } else if (original.getSchemaOfAdditionalProperties() == null &&
                update.getSchemaOfAdditionalProperties() != null) {
                ctx.addDifference(ADDITIONAL_PROPERTIES_NARROWED);
            } else if (update.getSchemaOfAdditionalProperties() == null &&
                original.getSchemaOfAdditionalProperties() != null) {
                ctx.addDifference(ADDITIONAL_PROPERTIES_EXTENDED);
            } else {
                SchemaDiff.compare(
                    ctx, original.getSchemaOfAdditionalProperties(), update.getSchemaOfAdditionalProperties());
            }
        }
    }

    private static void compareDependencies(
        final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        try (Context.PathScope pathScope = ctx.enterPath("dependencies")) {
            Set<String> propertyKeys = new HashSet<>(original.getPropertyDependencies().keySet());
            propertyKeys.addAll(update.getPropertyDependencies().keySet());

            for (String propertyKey : propertyKeys) {
                try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
                    if (!update.getPropertyDependencies().containsKey(propertyKey)) {
                        ctx.addDifference(DEPENDENCY_ARRAY_REMOVED);
                    } else if (!original.getPropertyDependencies().containsKey(propertyKey)) {
                        ctx.addDifference(DEPENDENCY_ARRAY_ADDED);
                    } else {
                        Set<String> originalDependencies = original.getPropertyDependencies().get(propertyKey);
                        Set<String> updateDependencies = original.getPropertyDependencies().get(propertyKey);
                        if (originalDependencies.equals(updateDependencies)) {
                            if (updateDependencies.containsAll(originalDependencies)) {
                                ctx.addDifference(DEPENDENCY_ARRAY_EXTENDED);
                            } else if (originalDependencies.containsAll(updateDependencies)) {
                                ctx.addDifference(DEPENDENCY_ARRAY_NARROWED);
                            } else {
                                ctx.addDifference(DEPENDENCY_ARRAY_CHANGED);
                            }
                        }
                    }
                }
            }

            propertyKeys = new HashSet<>(original.getSchemaDependencies().keySet());
            propertyKeys.addAll(update.getSchemaDependencies().keySet());

            for (String propertyKey : propertyKeys) {
                try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
                    if (!update.getSchemaDependencies().containsKey(propertyKey)) {
                        ctx.addDifference(DEPENDENCY_SCHEMA_REMOVED);
                    } else if (!original.getSchemaDependencies().containsKey(propertyKey)) {
                        ctx.addDifference(DEPENDENCY_SCHEMA_ADDED);
                    } else {
                        SchemaDiff.compare(
                            ctx,
                            original.getSchemaDependencies().get(propertyKey),
                            update.getSchemaDependencies().get(propertyKey)
                        );
                    }
                }
            }
        }
    }

    private static void compareProperties(
        final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        try (Context.PathScope pathScope = ctx.enterPath("properties")) {
            Set<String> propertyKeys = new HashSet<>(original.getPropertySchemas().keySet());
            propertyKeys.addAll(update.getPropertySchemas().keySet());

            for (String propertyKey : propertyKeys) {
                try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
                    if (!update.getPropertySchemas().containsKey(propertyKey)) {
                        // We only consider the content model of the update
                        ctx.addDifference(isOpenContentModel(update) ?
                            PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL :
                            PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL);
                    } else if (!original.getPropertySchemas().containsKey(propertyKey)) {
                        // We only consider the content model of the original
                        if (isOpenContentModel(original)) {
                            ctx.addDifference(PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL);
                        } else {
                            if (update.getRequiredProperties().contains(propertyKey)) {
                                if (update.getPropertySchemas().get(propertyKey).hasDefaultValue()) {
                                    ctx.addDifference(REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_CLOSED_CONTENT_MODEL);
                                } else {
                                    ctx.addDifference(REQUIRED_PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL);
                                }
                            } else {
                                ctx.addDifference(OPTIONAL_PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL);
                            }
                        }
                    } else {
                        SchemaDiff.compare(
                            ctx,
                            original.getPropertySchemas().get(propertyKey),
                            update.getPropertySchemas().get(propertyKey)
                        );
                    }
                }
            }
        }
    }

    private static void compareRequired(
        final Context ctx, final ObjectSchema original, final ObjectSchema update) {
        try (Context.PathScope pathScope = ctx.enterPath("required")) {
            for (String propertyKey : original.getPropertySchemas().keySet()) {
                if (update.getPropertySchemas().containsKey(propertyKey)) {
                    try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
                        boolean originalRequired = original.getRequiredProperties().contains(propertyKey);
                        boolean updateRequired = update.getRequiredProperties().contains(propertyKey);
                        if (originalRequired && !updateRequired) {
                            ctx.addDifference(REQUIRED_ATTRIBUTE_REMOVED);
                        } else if (!originalRequired && updateRequired) {
                            if (update.getPropertySchemas().get(propertyKey).hasDefaultValue()) {
                                ctx.addDifference(REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED);
                            } else {
                                ctx.addDifference(REQUIRED_ATTRIBUTE_ADDED);
                            }
                        }
                    }
                }
            }
        }
    }

    private static boolean isOpenContentModel(final ObjectSchema schema) {
        return schema.getPatternProperties().size() > 0 || schema.permitsAdditionalProperties();
    }
}