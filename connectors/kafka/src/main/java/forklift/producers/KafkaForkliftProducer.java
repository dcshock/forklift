package forklift.producers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import forklift.connectors.ForkliftMessage;
import forklift.message.Header;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by afrieze on 2/27/17.
 */
public class KafkaForkliftProducer implements ForkliftProducerI {

    private final static String FIELD_PROPERTIES = "forkliftProperties";
    private final String topic;
    private final KafkaProducer<?, ?> kafkaProducer;
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private String stringSchema = "{\"type\":\"record\"," +
                                  "\"name\":\"ForkliftStringMessage\"," +
                                  "\"fields\":[{\"name\":\"forkliftMsg\",\"type\":\"string\"},{\"name\":\"forkliftProperties\",\"type\":\"string\"}]}";

    private String mapSchema = "{\"type\":\"record\"," +
                               "\"name\":\"ForkliftMapMessage\"," +
                               "\"fields\":[{\"name\":\"forkliftMapMsg\",\"type\":\"string\"},{\"name\":\"forkliftProperties\",\"type\":\"string\"}]}";

    private String jsonSchema = "{\"type\":\"record\"," +
                                "\"name\":\"ForkliftJsonMessage\"," +
                                "\"fields\":[{\"name\":\"forkliftJsonMsg\",\"type\":\"string\"},{\"name\":\"forkliftProperties\",\"type\":\"string\"}]}";

    private Map<Class<?>, Schema> avroSchemas = new ConcurrentHashMap<>();
    private Schema parsedStringSchema = null;
    private Schema parsedMapSchema = null;
    private Schema parsedJsonSchema = null;

    private Map<String, String> properties = new HashMap<>();

    public KafkaForkliftProducer(String topic, KafkaProducer<?, ?> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        Schema.Parser parser = new Schema.Parser();
        this.parsedMapSchema = parser.parse(mapSchema);
        this.parsedStringSchema = parser.parse(stringSchema);
        this.parsedJsonSchema = parser.parse(jsonSchema);
    }

    @Override
    public String send(String message) throws ProducerException {
        GenericRecord avroRecord = new GenericData.Record(parsedStringSchema);
        avroRecord.put("forkliftMsg", message);
        avroRecord.put(FIELD_PROPERTIES, this.formatMap(this.properties));
        ProducerRecord record = new ProducerRecord<>(topic, null, avroRecord);
        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
        return null;
    }

    @Override
    public String send(ForkliftMessage message) throws ProducerException {
        GenericRecord avroRecord = new GenericData.Record(parsedStringSchema);
        avroRecord.put("forkliftMsg", message.getMsg());

        Map<String, String> messageProperties = message.getProperties();
        for (String key : this.properties.keySet()) {
            if (!messageProperties.containsKey(key)) {
                Object value = properties.get(key);
                messageProperties.put(key, value == null ? null : value.toString());
            }
        }
        message.setProperties(messageProperties);
        avroRecord.put(FIELD_PROPERTIES, this.formatMap(messageProperties));
        ProducerRecord record = new ProducerRecord<>(topic, null, avroRecord);
        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
        return null;
    }

    private void populatAvroMessage(JsonNode values, GenericRecord record, Schema schema) {
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Schema fieldSchema = schema.getField(fieldName).schema();
            if (fieldSchema.getType() == Schema.Type.RECORD) {
                GenericRecord subRecord = new GenericData.Record(fieldSchema);
                populatAvroMessage(values.get(fieldName), subRecord, fieldSchema);
                record.put(fieldName, subRecord);
            } else {
                record.put(fieldName, values.get(fieldName).textValue());
            }
        }
    }

    @Override
    public String send(Object message) throws ProducerException {
        ProducerRecord record = null;
        if (message instanceof SpecificRecord) {

            Schema schema = ((SpecificRecord)message).getSchema();
            String originalJson = schema.toString(false);
            ObjectMapper mapper = new ObjectMapper();
            try {
                Schema modifiedSchema = avroSchemas.get(message.getClass());
                if (modifiedSchema == null) {
                    JsonNode propertiesField =
                                    mapper.readTree("{\"name\":\"forkliftProperties\",\"type\":\"string\", \"default\":\"\"}");
                    ObjectNode schemaNode = (ObjectNode)mapper.readTree(originalJson);
                    ArrayNode fieldsNode = (ArrayNode)schemaNode.get("fields");
                    fieldsNode.add(propertiesField);
                    schemaNode.set("fields", fieldsNode);
                    Schema.Parser parser = new Schema.Parser();
                    modifiedSchema = parser.parse(mapper.writeValueAsString(schemaNode));
                    avroSchemas.put(message.getClass(), modifiedSchema);
                }
                GenericRecord avroRecord = new GenericData.Record(modifiedSchema);
                ObjectNode messageNode = (ObjectNode)mapper.readTree(message.toString());
                messageNode.put(FIELD_PROPERTIES, this.formatMap(this.properties));
                //move messageNode into avroRecord
                populatAvroMessage(messageNode, avroRecord, modifiedSchema);
                record = new ProducerRecord<String, GenericRecord>(topic, null, avroRecord);

            } catch (IOException e) {
                throw new ProducerException("Error creating Kafka Message", e);
            }
        } else {
            try {
                GenericRecord avroRecord = new GenericData.Record(parsedJsonSchema);
                String json = mapper.writeValueAsString(message);
                avroRecord.put("forkliftJsonMsg", json);
                avroRecord.put(FIELD_PROPERTIES, this.formatMap(this.properties));
                record = new ProducerRecord<>(topic, null, avroRecord);
            } catch (Exception e) {
                throw new ProducerException("Error creating Kafka Message", e);
            }
        }
        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
        return null;
    }

    @Override
    public String send(Map<String, String> message) throws ProducerException {
        GenericRecord avroRecord = new GenericData.Record(parsedMapSchema);

        String formattedMap = this.formatMap(message);
        avroRecord.put("forkliftMapMsg", formattedMap);
        avroRecord.put(FIELD_PROPERTIES, this.formatMap(this.properties));
        ProducerRecord record = new ProducerRecord<>(topic, null, avroRecord);
        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
        return null;
    }

    @Override
    public String send(Map<Header, Object> headers, Map<String, String> properties, ForkliftMessage message)
                    throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public String send(Map<String, String> properties, ForkliftMessage message)
                    throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public Map<String, String> getProperties() throws ProducerException {
        return this.properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) throws ProducerException {
        this.properties = properties;
    }

    @Override
    public void setHeaders(Map<Header, Object> haeders) {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public Map<Header, Object> getHeaders() {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public void close() throws IOException {
        //do nothing, the passed in KafkaProducer may be used elsewhere and should be closed by the KafkaController
    }

    private String formatMap(Map<String, ? extends Object> map) {
        StringBuilder builder = new StringBuilder();
        String
                        flattened =
                        map.entrySet()
                           .stream()
                           .map(entry -> entry.getKey() + "=" + (entry.getValue() == null ? "" : entry.getValue().toString()))
                           .collect(Collectors.joining("\n"));
        return flattened;
    }

}
