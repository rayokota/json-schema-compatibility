package io.yokota.json.diff;

import org.everit.json.schema.Schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

public class Context {
    private final Set<Difference.Type> compatibleChanges;
    private final Set<Schema> schemas;
    private final Deque<String> jsonPath;
    private final List<Difference> diffs;

    public Context(Set<Difference.Type> compatibleChanges) {
        this.compatibleChanges = compatibleChanges;
        this.schemas = Collections.newSetFromMap(new IdentityHashMap<>());
        this.jsonPath = new ArrayDeque<>();
        this.diffs = new ArrayList<>();
    }

    public Context getSubcontext() {
        Context ctx = new Context(this.compatibleChanges);
        ctx.schemas.addAll(this.schemas);
        ctx.jsonPath.addAll(this.jsonPath);
        return ctx;
    }

    public SchemaScope enterSchema(final Schema schema) {
        return !schemas.contains(schema) ? new SchemaScope(schema) : null;
    }

    public class SchemaScope implements AutoCloseable {
        private final Schema schema;

        public SchemaScope(final Schema schema) {
            this.schema = schema;
            schemas.add(schema);
        }

        public void close() {
            schemas.remove(schema);
        }
    }

    public PathScope enterPath(final String path) {
        return new PathScope(path);
    }

    public class PathScope implements AutoCloseable {
        public PathScope(final String path) {
            jsonPath.addLast(path);
        }

        public void close() {
            jsonPath.removeLast();
        }
    }

    public boolean isCompatible() {
        boolean notCompatible = getDifferences().stream()
            .map(Difference::getType)
            .anyMatch(t -> !compatibleChanges.contains(t));
        return !notCompatible;
    }

    public List<Difference> getDifferences() {
        return diffs;
    }

    public void addDifference(final Difference.Type type) {
        diffs.add(new Difference(type, jsonPathString(jsonPath)));
    }

    public void addDifference(final String attribute, final Difference.Type type) {
        jsonPath.addLast(attribute);
        addDifference(type);
        jsonPath.removeLast();
    }

    public void addDifferences(final List<Difference> differences) {
        diffs.addAll(differences);
    }

    private static String jsonPathString(final Deque<String> jsonPath) {
        return "#/" + String.join("/", jsonPath);
    }
}
