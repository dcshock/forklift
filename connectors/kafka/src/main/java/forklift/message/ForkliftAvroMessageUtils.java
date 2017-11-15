package forklift.message;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import forklift.connectors.KafkaSerializer;
import org.apache.avro.Schema;

import java.io.IOException;

public class ForkliftAvroMessageUtils {

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static Schema addForkliftPropertiesToSchema(Schema schema) throws IOException {
        String originalJson = schema.toString(false);
        JsonNode propertiesField = mapper.readTree(KafkaSerializer.SCHEMA_FIELD_VALUE_PROPERTIES);
        ObjectNode schemaNode = (ObjectNode) mapper.readTree(originalJson);
        ArrayNode fieldsNode = (ArrayNode) schemaNode.get("fields");
        fieldsNode.add(propertiesField);
        schemaNode.set("fields", fieldsNode);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(mapper.writeValueAsString(schemaNode));
    }

}
