package io.yokota.json.diff;

import org.everit.json.schema.ReferenceSchema;

public class ReferenceSchemaDiff {
    static void compare(
        final Context ctx, final ReferenceSchema original, final ReferenceSchema update) {
        try (Context.PathScope pathScope2 = ctx.enterPath("$ref")) {
            SchemaDiff.compare(ctx, original.getReferredSchema(), update.getReferredSchema());
        }
    }
}
