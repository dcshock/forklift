package forklift.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
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
 * Implementation of the {@link forklift.producers.ForkliftProducerI}.  Messages sent are fully integrated
 * with confluent's schema-registry.  Avro compiled objects may be sent through the {@link #send(Object)} method.  If
 * an avro object is sent, the schema will be evolved to include the {@link #SCHEMA_FIELD_NAME_PROPERTIES} field as
 * follows:
 * <pre>
 * {"name":"forkliftProperties","type":"string","default":""}
 * </pre>
 * <p>
 * The value of the forkliftProperties will be key,value entries delimited with a newline
 * <p>
 * <strong>Example: </strong>
 * <pre>
 *     key1,value1
 *     key2,value2
 * </pre>
 * <p>
 * Non-avro messages are sent with the following schema
 * <pre>
 *   {"type":"record","name":"ForkliftMessage","fields":[{"name":"forkliftValue","type":"string", "default":""},
 *                                                       {"name":"forkliftProperties","type":"string","default":""}]}
 * </pre>
 * <p>
 * Headers are not supported and calls to the {@link #send(java.util.Map, java.util.Map, forklift.connectors.ForkliftMessage)}
 * or {@link #setHeaders(java.util.Map)} will result in an {@link java.lang.UnsupportedOperationException}.
 */
public class KafkaForkliftProducer implements ForkliftProducerI {

    public final static String SCHEMA_FIELD_NAME_VALUE = "forkliftValue";
    public final static String SCHEMA_FIELD_NAME_PROPERTIES = "forkliftProperties";
    private final String topic;
    private final KafkaProducer<?, ?> kafkaProducer;
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Schema forkliftSchema = null;
    private Map<Class<?>, Schema> avroSchemaCache = new ConcurrentHashMap<>();
    private Map<String, String> properties = new HashMap<>();

    public KafkaForkliftProducer(String topic, KafkaProducer<?, ?> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        Schema.Parser parser = new Schema.Parser();
        this.forkliftSchema = parser.parse(
                        "{\"type\":\"record\",\"name\":\"ForkliftMessage\",\"fields\":" +
                        "[{\"name\":\"forkliftValue\",\"type\":\"string\",\"default\":\"\"}," +
                        "{\"name\":\"forkliftProperties\",\"type\":\"string\",\"default\":\"\"}]}");
    }

    @Override
    public String send(String message) throws ProducerException {
        sendForkliftWrappedMessage(message, null);
        return null;
    }

    @Override
    public String send(ForkliftMessage message) throws ProducerException {
        sendForkliftWrappedMessage(message.getMsg(), message.getProperties());
        return null;
    }

    @Override
    public String send(Object message) throws ProducerException {
        if (message instanceof SpecificRecord) {
            sendAvroMessage((SpecificRecord)message);
        } else {
            String json;
            try {
                json = mapper.writeValueAsString(message);
            } catch (JsonProcessingException e) {
                throw new ProducerException("Error creating Kafka Message", e);
            }
            sendForkliftWrappedMessage(json, null);
        }
        return null;
    }

    @Override
    public String send(Map<String, String> message) throws ProducerException {
        sendForkliftWrappedMessage(this.formatMap(message), null);
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
        this.sendForkliftWrappedMessage(message.getMsg(), properties);
        return null;
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

    private void sendForkliftWrappedMessage(String message, Map<String, String> messageProperties) throws ProducerException {
        GenericRecord avroRecord = new GenericData.Record(forkliftSchema);
        avroRecord.put(SCHEMA_FIELD_NAME_VALUE, message);
        messageProperties = messageProperties == null ? new HashMap<>() : new HashMap<>(messageProperties);
        //add the producer level properties but do not overwrite message level properties
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            messageProperties.putIfAbsent(entry.getKey(), entry.getValue());
        }
        avroRecord.put(SCHEMA_FIELD_NAME_PROPERTIES, this.formatMap(messageProperties));
        ProducerRecord record = new ProducerRecord<>(topic, null, avroRecord);
        try {
            kafkaProducer.send(record);

        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
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

    private Schema addForkliftPropertiesToSchema(Schema schema) throws IOException {
        String originalJson = schema.toString(false);
        JsonNode propertiesField = mapper.readTree("{\"name\":\"forkliftProperties\",\"type\":\"string\",\"default\":\"\"}");
        ObjectNode schemaNode = (ObjectNode)mapper.readTree(originalJson);
        ArrayNode fieldsNode = (ArrayNode)schemaNode.get("fields");
        fieldsNode.add(propertiesField);
        schemaNode.set("fields", fieldsNode);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(mapper.writeValueAsString(schemaNode));
    }

    private void sendAvroMessage(SpecificRecord message) throws ProducerException {
        try {
            Schema modifiedSchema = avroSchemaCache.get(message.getClass());
            if (modifiedSchema == null) {
                modifiedSchema = addForkliftPropertiesToSchema(message.getSchema());
                avroSchemaCache.put(message.getClass(), modifiedSchema);
            }
            GenericRecord avroRecord = new GenericData.Record(modifiedSchema);
            ObjectNode messageNode = (ObjectNode)mapper.readTree(message.toString());
            messageNode.put(SCHEMA_FIELD_NAME_PROPERTIES, this.formatMap(this.properties));
            //move messageNode into avroRecord
            populatAvroMessage(messageNode, avroRecord, modifiedSchema);
            ProducerRecord record = new ProducerRecord<String, GenericRecord>(topic, null, avroRecord);
            try {
                kafkaProducer.send(record);
            } catch (SerializationException e) {
                throw new ProducerException("Error creating Kafka Message", e);
            }
        } catch (IOException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
    }

    private String formatMap(Map<String, String> map) {
        return map.entrySet()
                  .stream()
                  .map(entry -> entry.getKey() + "=" + (entry.getValue() == null ? "" : entry.getValue()))
                  .collect(Collectors.joining("\n"));
    }

}
