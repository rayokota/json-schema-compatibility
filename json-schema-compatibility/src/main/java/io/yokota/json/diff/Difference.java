package io.yokota.json.diff;

import java.util.Objects;

public class Difference {
    public enum Type {
        ID_CHANGED,
        DESCRIPTION_CHANGED,
        TITLE_CHANGED,
        DEFAULT_CHANGED,
        SCHEMA_REMOVED,
        TYPE_EXTENDED,
        TYPE_NARROWED,
        TYPE_CHANGED,

        MAX_LENGTH_ADDED,
        MAX_LENGTH_REMOVED,
        MAX_LENGTH_INCREASED,
        MAX_LENGTH_DECREASED,
        MIN_LENGTH_ADDED,
        MIN_LENGTH_REMOVED,
        MIN_LENGTH_INCREASED,
        MIN_LENGTH_DECREASED,
        PATTERN_ADDED,
        PATTERN_REMOVED,
        PATTERN_CHANGED,

        MAXIMUM_ADDED,
        MAXIMUM_REMOVED,
        MAXIMUM_INCREASED,
        MAXIMUM_DECREASED,
        MINIMUM_ADDED,
        MINIMUM_REMOVED,
        MINIMUM_INCREASED,
        MINIMUM_DECREASED,
        EXCLUSIVE_MAXIMUM_ADDED,
        EXCLUSIVE_MAXIMUM_REMOVED,
        EXCLUSIVE_MAXIMUM_INCREASED,
        EXCLUSIVE_MAXIMUM_DECREASED,
        EXCLUSIVE_MINIMUM_ADDED,
        EXCLUSIVE_MINIMUM_REMOVED,
        EXCLUSIVE_MINIMUM_INCREASED,
        EXCLUSIVE_MINIMUM_DECREASED,
        MULTIPLE_OF_ADDED,
        MULTIPLE_OF_REMOVED,
        MULTIPLE_OF_EXPANDED,
        MULTIPLE_OF_REDUCED,
        MULTIPLE_OF_CHANGED,

        REQUIRED_ATTRIBUTE_ADDED,
        REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED,
        REQUIRED_ATTRIBUTE_REMOVED,
        MAX_PROPERTIES_ADDED,
        MAX_PROPERTIES_REMOVED,
        MAX_PROPERTIES_INCREASED,
        MAX_PROPERTIES_DECREASED,
        MIN_PROPERTIES_ADDED,
        MIN_PROPERTIES_REMOVED,
        MIN_PROPERTIES_INCREASED,
        MIN_PROPERTIES_DECREASED,
        ADDITIONAL_PROPERTIES_ADDED,
        ADDITIONAL_PROPERTIES_REMOVED,
        ADDITIONAL_PROPERTIES_EXTENDED,
        ADDITIONAL_PROPERTIES_NARROWED,
        DEPENDENCY_ARRAY_ADDED,
        DEPENDENCY_ARRAY_REMOVED,
        DEPENDENCY_ARRAY_EXTENDED,
        DEPENDENCY_ARRAY_NARROWED,
        DEPENDENCY_ARRAY_CHANGED,
        DEPENDENCY_SCHEMA_ADDED,
        DEPENDENCY_SCHEMA_REMOVED,
        PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL,
        REQUIRED_PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL,
        REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_CLOSED_CONTENT_MODEL,
        OPTIONAL_PROPERTY_ADDED_TO_CLOSED_CONTENT_MODEL,
        PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL,
        PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL,

        MAX_ITEMS_ADDED,
        MAX_ITEMS_REMOVED,
        MAX_ITEMS_INCREASED,
        MAX_ITEMS_DECREASED,
        MIN_ITEMS_ADDED,
        MIN_ITEMS_REMOVED,
        MIN_ITEMS_INCREASED,
        MIN_ITEMS_DECREASED,
        UNIQUE_ITEMS_ADDED,
        UNIQUE_ITEMS_REMOVED,
        ADDITIONAL_ITEMS_ADDED,
        ADDITIONAL_ITEMS_REMOVED,
        ADDITIONAL_ITEMS_EXTENDED,
        ADDITIONAL_ITEMS_NARROWED,
        ITEMS_ADDED_TO_OPEN_CONTENT_MODEL,
        ITEMS_ADDED_TO_CLOSED_CONTENT_MODEL,
        ITEMS_REMOVED_FROM_OPEN_CONTENT_MODEL,
        ITEMS_REMOVED_FROM_CLOSED_CONTENT_MODEL,

        ENUM_ARRAY_EXTENDED,
        ENUM_ARRAY_NARROWED,
        ENUM_ARRAY_CHANGED,

        COMPOSITION_METHOD_CHANGED,
        PRODUCT_TYPE_EXTENDED,
        PRODUCT_TYPE_NARROWED,
        SUM_TYPE_EXTENDED,
        SUM_TYPE_NARROWED
    }

    private final String jsonPath;
    private final Type type;

    public Difference(final Type type, final String jsonPath) {
        this.jsonPath = jsonPath;
        this.type = type;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Difference that = (Difference) o;
        return Objects.equals(jsonPath, that.jsonPath) &&
            type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonPath, type);
    }

    @Override
    public String toString() {
        return "Difference{" +
            "jsonPath='" + jsonPath + '\'' +
            ", type=" + type +
            '}';
    }
}
