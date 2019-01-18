package io.yokota.json.diff;

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.Schema;

import java.util.Iterator;

import static io.yokota.json.diff.Difference.Type.*;

class CombinedSchemaDiff {
    static void compare(final Context ctx, final CombinedSchema original, final CombinedSchema update) {
        if (!original.getCriterion().equals(update.getCriterion())) {
            ctx.addDifference(COMPOSITION_METHOD_CHANGED);
        } else {
            int originalSize = original.getSubschemas().size();
            int updateSize = update.getSubschemas().size();
            if (originalSize < updateSize) {
                if (original.getCriterion() == CombinedSchema.ALL_CRITERION) {
                    ctx.addDifference(PRODUCT_TYPE_EXTENDED);
                } else {
                    ctx.addDifference(SUM_TYPE_EXTENDED);
                }
            } else if (originalSize > updateSize) {
                if (original.getCriterion() == CombinedSchema.ALL_CRITERION) {
                    ctx.addDifference(PRODUCT_TYPE_NARROWED);
                } else {
                    ctx.addDifference(SUM_TYPE_NARROWED);
                }
            }

            final Iterator<Schema> originalIterator = original.getSubschemas().iterator();
            final Iterator<Schema> updateIterator = update.getSubschemas().iterator();
            int index = 0;
            while (originalIterator.hasNext() && index < Math.min(originalSize, updateSize)) {
                try (Context.PathScope pathScope = ctx.enterPath(original.getCriterion() + "/" + index)) {
                    SchemaDiff.compare(ctx, originalIterator.next(), updateIterator.next());
                }
                index++;
            }
        }
    }
}
