package io.yokota.json.diff;

import org.everit.json.schema.NumberSchema;

import java.util.Objects;

import static io.yokota.json.diff.Difference.Type.*;

class NumberSchemaDiff {
    static void compare(final Context ctx, final NumberSchema original, final NumberSchema update) {
        if (!Objects.equals(original.getMaximum(), update.getMaximum())) {
            if (original.getMaximum() == null && update.getMaximum() != null) {
                ctx.addDifference("maximum", MAXIMUM_ADDED);
            } else if (original.getMaximum() != null && update.getMaximum() == null) {
                ctx.addDifference("maximum", MAXIMUM_REMOVED);
            } else if (original.getMaximum().doubleValue() < update.getMaximum().doubleValue()) {
                ctx.addDifference("maximum", MAXIMUM_INCREASED);
            } else if (original.getMaximum().doubleValue() > update.getMaximum().doubleValue()) {
                ctx.addDifference("maximum", MAXIMUM_DECREASED);
            }
        }
        if (!Objects.equals(original.getMinimum(), update.getMinimum())) {
            if (original.getMinimum() == null && update.getMinimum() != null) {
                ctx.addDifference("minimum", MINIMUM_ADDED);
            } else if (original.getMinimum() != null && update.getMinimum() == null) {
                ctx.addDifference("minimum", MINIMUM_REMOVED);
            } else if (original.getMinimum().doubleValue() < update.getMinimum().doubleValue()) {
                ctx.addDifference("minimum", MINIMUM_INCREASED);
            } else if (original.getMinimum().doubleValue() > update.getMinimum().doubleValue()) {
                ctx.addDifference("minimum", MINIMUM_DECREASED);
            }
        }
        if (!Objects.equals(original.getExclusiveMaximumLimit(), update.getExclusiveMaximumLimit())) {
            if (original.getExclusiveMaximumLimit() == null && update.getExclusiveMaximumLimit() != null) {
                ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_ADDED);
            } else if (original.getExclusiveMaximumLimit() != null && update.getExclusiveMaximumLimit() == null) {
                ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_REMOVED);
            } else if (original.getExclusiveMaximumLimit().doubleValue() < update.getExclusiveMaximumLimit().doubleValue()) {
                ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_INCREASED);
            } else if (original.getExclusiveMaximumLimit().doubleValue() > update.getMaximum().doubleValue()) {
                ctx.addDifference("exclusiveMaximum", EXCLUSIVE_MAXIMUM_DECREASED);
            }
        }
        if (!Objects.equals(original.getExclusiveMinimumLimit(), update.getExclusiveMinimumLimit())) {
            if (original.getExclusiveMinimumLimit() == null && update.getExclusiveMinimumLimit() != null) {
                ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_ADDED);
            } else if (original.getExclusiveMinimumLimit() != null && update.getExclusiveMinimumLimit() == null) {
                ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_REMOVED);
            } else if (original.getExclusiveMinimumLimit().doubleValue() < update.getExclusiveMinimumLimit().doubleValue()) {
                ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_INCREASED);
            } else if (original.getExclusiveMinimumLimit().doubleValue() > update.getExclusiveMinimumLimit().doubleValue()) {
                ctx.addDifference("exclusiveMinimum", EXCLUSIVE_MINIMUM_DECREASED);
            }
        }
        if (!Objects.equals(original.getMultipleOf(), update.getMultipleOf())) {
            if (original.getMultipleOf() == null && update.getMultipleOf() != null) {
                ctx.addDifference("multipleOf", MULTIPLE_OF_ADDED);
            } else if (original.getMultipleOf() != null && update.getMultipleOf() == null) {
                ctx.addDifference("multipleOf", MULTIPLE_OF_REMOVED);
            } else if (update.getMultipleOf().intValue() % original.getMultipleOf().intValue() == 0) {
                ctx.addDifference("multipleOf", MULTIPLE_OF_EXPANDED);
            } else if (original.getMultipleOf().intValue() % update.getMultipleOf().intValue() == 0) {
                ctx.addDifference("multipleOf", MULTIPLE_OF_REDUCED);
            } else {
                ctx.addDifference("multipleOf", MULTIPLE_OF_CHANGED);
            }
        }
        if (original.requiresInteger() != update.requiresInteger()) {
            if (original.requiresInteger()) {
                ctx.addDifference(TYPE_EXTENDED);
            } else {
                ctx.addDifference(TYPE_NARROWED);
            }
        }
    }
}