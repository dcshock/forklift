package forklift.integration;

import static org.junit.Assert.assertTrue;
import forklift.connectors.ConnectorException;
import forklift.connectors.KafkaConnector;
import forklift.connectors.Person;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration tests to test actually sending messages to a kafka instance.  Please modify the setup method
 * to match your own kafka and confluent schema registry locations.
 */
public class IntegrationTest {

    private KafkaConnector connector;

    @Before
    public void setup() throws ConnectorException {
        this.connector = new KafkaConnector("localhost:29092", "http://localhost:28081", "app");
        connector.start();
    }

    @Ignore
    @After
    public void teardown() throws ConnectorException {
        connector.stop();
    }

    @Ignore
    @Test
    public void producerStringTests() throws ProducerException, ConnectorException {
        ForkliftProducerI producer = connector.getTopicProducer("forklift-stringTopic");
        producer.send("Test Message");
        assertTrue(true);
    }

    @Ignore
    @Test
    public void producerMapTests() throws ProducerException, ConnectorException {
        Map<String, String> values = new HashMap<String, String>();
        values.put("value1", "1");
        values.put("value2", "2");
        ForkliftProducerI producer = connector.getTopicProducer("forklift-mapTopic");
        producer.send(values);
        assertTrue(true);
    }

    @Ignore
    @Test
    public void producerPersonTest() throws ProducerException, ConnectorException {
        Person person = new Person("John", "Doe");
        ForkliftProducerI producer = connector.getTopicProducer("forklift-pojoTopic");
        producer.send(person);
        assertTrue(true);
    }

    @Ignore
    @Test
    public void multipleRecordsAndTopics() throws ProducerException, ConnectorException {
        ForkliftProducerI pojoProducer = connector.getTopicProducer("forklift-pojoTopic");
        ForkliftProducerI stringProducer = connector.getTopicProducer("forklift-stringTopic");
        for (int i = 0; i < 10000; i++) {
            Person person = new Person("John" + i, "Doe" + i);
            //pojoProducer.send(person);
            stringProducer.send("Test Message" + i);
        }
        assertTrue(true);
    }
}
