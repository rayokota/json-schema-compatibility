package io.yokota.json.diff;

import org.everit.json.schema.EnumSchema;

import static io.yokota.json.diff.Difference.Type.ENUM_ARRAY_CHANGED;
import static io.yokota.json.diff.Difference.Type.ENUM_ARRAY_EXTENDED;
import static io.yokota.json.diff.Difference.Type.ENUM_ARRAY_NARROWED;

class EnumSchemaDiff {
    static void compare(
        final Context ctx, final EnumSchema original, final EnumSchema update) {
        if (!original.getPossibleValues().equals(update.getPossibleValues())) {
            if (update.getPossibleValues().containsAll(original.getPossibleValues())) {
                ctx.addDifference("enum", ENUM_ARRAY_EXTENDED);
            } else if (original.getPossibleValues().containsAll(update.getPossibleValues())) {
                ctx.addDifference("enum", ENUM_ARRAY_NARROWED);
            } else {
                ctx.addDifference("enum", ENUM_ARRAY_CHANGED);
            }
        }
    }
}
