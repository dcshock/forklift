package forklift.connectors;

import static org.junit.Assert.assertTrue;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by afrieze on 3/1/17.
 */
public class IntegrationTest {

    @Test
    public void producerStringTests() throws ProducerException, ConnectorException {
        KafkaConnector connector = new KafkaConnector("localhost:29092", "http://localhost:28081", "app");
        connector.start();
        ForkliftProducerI producer = connector.getTopicProducer("forklift-stringTopic");
        producer.send("Test Message");
        assertTrue(true);
        connector.stop();
    }

    @Test
    public void producerMapTests() throws ProducerException, ConnectorException {
        KafkaConnector connector = new KafkaConnector("localhost:29092", "http://localhost:28081", "app");
        connector.start();
        Map<String, String> values = new HashMap<String, String>();
        values.put("value1", "1");
        values.put("value2", "2");
        ForkliftProducerI producer = connector.getTopicProducer("forklift-mapTopic");
        producer.send(values);
        assertTrue(true);
        connector.stop();
    }

    @Test
    public void producerObjectTest() throws ProducerException, ConnectorException {
        com.sofi.avro.schemas.Test value = new com.sofi.avro.schemas.Test();
        value.setName("TheName");
        KafkaConnector connector = new KafkaConnector("localhost:29092", "http://localhost:28081", "app");
        connector.start();
        ForkliftProducerI producer = connector.getTopicProducer("forklift-objectTopic");
        producer.send(value);
        assertTrue(true);
        connector.stop();
    }

    @Test
    public void producerPersonTest() throws ProducerException, ConnectorException {
        Person person = new Person("John", "Doe");
        KafkaConnector connector = new KafkaConnector("localhost:29092", "http://localhost:28081", "app");
        connector.start();
        ForkliftProducerI producer = connector.getTopicProducer("forklift-pojoTopic");
        producer.send(person);
        assertTrue(true);
        connector.stop();
    }
}
