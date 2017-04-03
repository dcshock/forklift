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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link forklift.producers.ForkliftProducerI}.  Messages sent are fully integrated
 * with confluent's schema-registry.  Avro compiled objects may be sent through the {@link #send(Object)} method.  If
 * an avro object is sent, the schema will be evolved to include the {@link #SCHEMA_FIELD_NAME_PROPERTIES} field as
 * follows:
 * <pre>
 * {"name":"forkliftProperties","type":"string","default":"",
 *  "doc":"Properties added to support forklift interfaces. Format is key,value entries delimited with new lines"}
 * </pre>
 * <p>
 * The value of the forkliftProperties will be key=value entries delimited with a newline
 * <p>
 * <strong>Example: </strong>
 * <pre>
 *     key1=value1
 *     key2=value2
 * </pre>
 * <p>
 * Non-avro messages are sent with the following schema
 * <pre>
 *   {"type":"record",
 *    "name":"ForkliftMessage",
 *    "doc":"Non-Avro messages sent through forklift use this schema."
 *    "fields":[{"name":"forkliftValue",
 *               "type":"string",
 *               "default":"",
 *               "doc":"The forklift message.  3 formats are supported.  1: string value, 2: Json object,
 *                      3: Map represented by key=value entries delimited with newline"},
 *              {"name":"forkliftProperties",
 *               "type":"string",
 *               "default":"",
 *               "doc":"Properties added to support forklift interfaces. Format is key=value entries delimited with new lines"}]}
 * </pre>
 * <p>
 * Headers are not supported and calls to the {@link #send(java.util.Map, java.util.Map, forklift.connectors.ForkliftMessage)}
 * or {@link #setHeaders(java.util.Map)} will result in an {@link java.lang.UnsupportedOperationException}.
 */
public class KafkaForkliftProducer implements ForkliftProducerI {

    public final static String SCHEMA_FIELD_NAME_VALUE = "forkliftValue";
    public final static String SCHEMA_FIELD_NAME_PROPERTIES = "forkliftProperties";

    private final static String SCHEMA_FIELD_VALUE_PROPERTIES =
                    "{\"name\":\"forkliftProperties\",\"type\":\"string\",\"default\":\"\"," +
                    "\"doc\":\"Properties added to support forklift interfaces. Format is key,value entries delimited with new lines\"}";

    private final String topic;
    private final KafkaProducer<?, ?> kafkaProducer;
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                                 .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Map<Class<?>, Schema> avroSchemaCache = new ConcurrentHashMap<>();
    private Schema forkliftSchema = null;
    private Map<String, String> properties = new HashMap<>();

    public KafkaForkliftProducer(String topic, KafkaProducer<?, ?> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
        Schema.Parser parser = new Schema.Parser();
        this.forkliftSchema = parser.parse(
                        "{\"type\":\"record\",\"name\":\"ForkliftMessage\"," +
                        " \"doc\":\"Non-Avro messages sent through forklift use this schema.\",\"fields\":" +
                        "[{\"name\":\"forkliftValue\",\"type\":\"string\",\"default\":\"\", \"doc\":\"The forklift message.  " +
                        "3 formats are supported.  1: string value, 2: Json object," +
                        "3: Map represented by key,value entries delimited with newline\"}," +
                        "{\"name\":\"forkliftProperties\",\"type\":\"string\",\"default\":\"\"," +
                        "\"doc\":\"Properties added to support forklift interfaces. Format is key,value entries delimited with new lines\"}]}");
    }

    @Override
    public String send(String message) throws ProducerException {
        return sendForkliftWrappedMessage(message, null);
    }

    @Override
    public String send(ForkliftMessage message) throws ProducerException {
        return sendForkliftWrappedMessage(message.getMsg(), message.getProperties());
    }

    @Override
    public String send(Object message) throws ProducerException {
        if (message instanceof SpecificRecord) {
            return sendAvroMessage((SpecificRecord)message);
        } else {
            String json;
            try {
                json = mapper.writeValueAsString(message);
            } catch (JsonProcessingException e) {
                throw new ProducerException("Error creating Kafka Message", e);
            }
            return sendForkliftWrappedMessage(json, null);
        }
    }

    @Override
    public String send(Map<String, String> message) throws ProducerException {
        return sendForkliftWrappedMessage(this.formatMap(message), null);
    }

    @Override
    public String send(Map<Header, Object> headers, Map<String, String> properties, ForkliftMessage message)
                    throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public String send(Map<String, String> properties, ForkliftMessage message)
                    throws ProducerException {
        return this.sendForkliftWrappedMessage(message.getMsg(), properties);
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

    private String sendForkliftWrappedMessage(String message, Map<String, String> messageProperties) throws ProducerException {
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
            RecordMetadata result = (RecordMetadata)kafkaProducer.send(record).get();
            return result.topic() + "-" + result.partition() + "-" + result.offset();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProducerException("Error sending Kafka Message", e);
        } catch (ExecutionException e) {
            throw new ProducerException("Error sending Kafka Message", e);
        }
    }

    private Schema addForkliftPropertiesToSchema(Schema schema) throws IOException {
        String originalJson = schema.toString(false);
        JsonNode propertiesField = mapper.readTree(SCHEMA_FIELD_VALUE_PROPERTIES);
        ObjectNode schemaNode = (ObjectNode)mapper.readTree(originalJson);
        ArrayNode fieldsNode = (ArrayNode)schemaNode.get("fields");
        fieldsNode.add(propertiesField);
        schemaNode.set("fields", fieldsNode);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(mapper.writeValueAsString(schemaNode));
    }

    private GenericRecord addForkliftPropertiesToAvroObject(SpecificRecord message) throws IOException {
        //Write message to json
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(message.getSchema(), outputStream);
        DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(message.getSchema());
        writer.write(message, encoder);
        encoder.flush();
        String json = new String(outputStream.toByteArray(), Charset.forName("UTF-8"));

        //modify schema to include forklift properties
        Schema modifiedSchema = avroSchemaCache.get(message.getClass());
        if (modifiedSchema == null) {
            modifiedSchema = addForkliftPropertiesToSchema(message.getSchema());
            avroSchemaCache.put(message.getClass(), modifiedSchema);
        }

        //add forklift properties to json
        ObjectNode messageNode = (ObjectNode)mapper.readTree(json);
        messageNode.put(SCHEMA_FIELD_NAME_PROPERTIES, this.formatMap(this.properties));

        //read modified json to avro object with modified schema
        InputStream input = new ByteArrayInputStream(messageNode.toString().getBytes(Charset.forName("UTF-8")));
        DataInputStream din = new DataInputStream(input);
        Decoder decoder = DecoderFactory.get().jsonDecoder(modifiedSchema, din);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(modifiedSchema);
        return reader.read(null, decoder);
    }

    private String sendAvroMessage(SpecificRecord message) throws ProducerException {
        try {
            ProducerRecord record = null;
            if(this.properties.size() > 0){
                GenericRecord avroRecord = addForkliftPropertiesToAvroObject(message);
                record = new ProducerRecord<String, GenericRecord>(topic, null, avroRecord);
            }
            else{
                record = new ProducerRecord<String, SpecificRecord>(topic, null, message);
            }
            try {
                RecordMetadata result = (RecordMetadata)kafkaProducer.send(record).get();
                return result.topic() + "-" + result.partition() + "-" + result.offset();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProducerException("Error sending Kafka Message", e);
            } catch (ExecutionException e) {
                throw new ProducerException("Error sending Kafka Message", e);
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
