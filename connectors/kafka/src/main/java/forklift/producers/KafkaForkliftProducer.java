package forklift.producers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by afrieze on 2/27/17.
 */
public class KafkaForkliftProducer implements ForkliftProducerI {

    private final String topic;
    private final KafkaProducer<?, ?> kafkaProducer;
    static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                                   .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    String stringSchema = "{\"type\":\"record\"," +
                          "\"name\":\"ForkliftStringMessage\"," +
                          "\"fields\":[{\"name\":\"forkliftMsg\",\"type\":\"string\"}]}";

    String mapSchema = "{\"type\":\"record\"," +
                       "\"name\":\"ForkliftMapMessage\"," +
                       "\"fields\":[{\"name\":\"forkliftMapMsg\",\"type\":\"string\"}]}";

    String jsonSchema = "{\"type\":\"record\"," +
                       "\"name\":\"ForkliftJsonMessage\"," +
                       "\"fields\":[{\"name\":\"forkliftJsonMsg\",\"type\":\"string\"}]}";

    Schema parsedStringSchema = null;
    Schema parsedMapSchema = null;
    Schema parsedJsonSchema = null;

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
        return this.send(message.getMsg());
    }

    @Override
    public String send(Object message) throws ProducerException {
        ProducerRecord record = null;
        if(message instanceof SpecificRecord){
            record = new ProducerRecord(topic, null, message);
        }
        else{
            try {
                GenericRecord avroRecord = new GenericData.Record(parsedJsonSchema);
                String json = mapper.writeValueAsString(message);
                avroRecord.put("forkliftJsonMsg", json);
                record = new ProducerRecord<>(topic, null, avroRecord);
            }catch(Exception e){
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

        String formattedMap = message.keySet().stream().map(key -> key + "=" + message.get(key)).collect(Collectors.joining("\n"));

        avroRecord.put("forkliftMapMsg", formattedMap);
        ProducerRecord record = new ProducerRecord<>(topic, null, avroRecord);
        try {
            kafkaProducer.send(record);
        } catch (SerializationException e) {
            throw new ProducerException("Error creating Kafka Message", e);
        }
        return null;
    }

    @Override
    public String send(Map<Header, Object> headers, Map<String, Object> properties, ForkliftMessage message)
                    throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers or properties");
    }

    @Override
    public Map<Header, Object> getHeaders() throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers");
    }

    @Override
    public void setHeaders(Map<Header, Object> headers) throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support headers");
    }

    @Override
    public Map<String, Object> getProperties() throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support propertied");
    }

    @Override
    public void setProperties(Map<String, Object> properties) throws ProducerException {
        throw new UnsupportedOperationException("Kafka Producer does not support properties");
    }

    @Override
    public void close() throws IOException {
        //do nothing, the passed in KafkaProducer may be used elsewhere and should be closed by the KafkaController
    }
}
