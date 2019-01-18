package io.yokota.json.diff;

import org.everit.json.schema.StringSchema;

import java.util.Objects;

import static io.yokota.json.diff.Difference.Type.*;

class StringSchemaDiff {
    static void compare(final Context ctx, final StringSchema original, final StringSchema update) {
        if (!Objects.equals(original.getMaxLength(), update.getMaxLength())) {
            if (original.getMaxLength() == null && update.getMaxLength() != null) {
                ctx.addDifference("maxLength", MAX_LENGTH_ADDED);
            } else if (original.getMaxLength() != null && update.getMaxLength() == null) {
                ctx.addDifference("maxLength", MAX_LENGTH_REMOVED);
            } else if (original.getMaxLength() < update.getMaxLength()) {
                ctx.addDifference("maxLength", MAX_LENGTH_INCREASED);
            } else if (original.getMaxLength() > update.getMaxLength()) {
                ctx.addDifference("maxLength", MAX_LENGTH_DECREASED);
            }
        }
        if (!Objects.equals(original.getMinLength(), update.getMinLength())) {
            if (original.getMinLength() == null && update.getMinLength() != null) {
                ctx.addDifference("minLength", MIN_LENGTH_ADDED);
            } else if (original.getMinLength() != null && update.getMinLength() == null) {
                ctx.addDifference("minLength", MIN_LENGTH_REMOVED);
            } else if (original.getMinLength() < update.getMinLength()) {
                ctx.addDifference("minLength", MIN_LENGTH_INCREASED);
            } else if (original.getMinLength() > update.getMinLength()) {
                ctx.addDifference("minLength", MIN_LENGTH_DECREASED);
            }
        }
        if (original.getPattern() == null && update.getPattern() != null) {
            ctx.addDifference("pattern", PATTERN_ADDED);
        } else if (original.getPattern() != null && update.getPattern() == null) {
            ctx.addDifference("pattern", PATTERN_REMOVED);
        } else if (original.getPattern() != null && update.getPattern() != null
            && !original.getPattern().pattern().equals(update.getPattern().pattern())) {
            ctx.addDifference("pattern", PATTERN_CHANGED);
        }
    }
}
