package io.yokota.json.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.everit.json.schema.JsonSchemaUtil;
import org.everit.json.schema.loader.JsonObject;

import java.io.InputStream;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResourceLoader {

    public static final ResourceLoader DEFAULT = new ResourceLoader("/io/yokota/json/connect/");

    private final String rootPath;

    public ResourceLoader(String rootPath) {
        this.rootPath = requireNonNull(rootPath, "rootPath cannot be null");
    }

    @SuppressWarnings("unchecked")
    public JsonObject readObj(String relPath) {
        InputStream stream = getStream(relPath);
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode value;
		try {
			value = (ObjectNode)objectMapper.readTree(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
       	Map map = JsonSchemaUtil.objectNodeToMap(value);
        return new JsonObject(map);
    }

    public InputStream getStream(String relPath) {
        String absPath = rootPath + relPath;
        InputStream rval = getClass().getResourceAsStream(absPath);
        if (rval == null) {
            throw new IllegalArgumentException(
                    format("failed to load resource by relPath [%s].\n"
                    + "InputStream by path [%s] is null", relPath, absPath));
        }
        return rval;
    }

}
