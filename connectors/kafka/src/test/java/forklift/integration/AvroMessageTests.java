package forklift.integration;

import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.Address;
import com.github.dcshock.avro.schemas.ComplexAvroMessage;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by afrieze on 3/13/17.
 */
public class AvroMessageTests {

    private static final Logger log = LoggerFactory.getLogger(AvroMessageTests.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;
    //    private static boolean ordered = true;
    private static boolean isPropsSet = false;

    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        called.set(0);
        isInjectNull = true;
    }

    @Test
    public void testAvroMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance("").getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-avro-topic");
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("Eye", "producerProperty");
        producer.setProperties(producerProps);
        for (int i = 0; i < msgCount; i++) {
            ComplexAvroMessage avroMessage = new ComplexAvroMessage();
            avroMessage.setAddress(new Address());
            avroMessage.getAddress().setCity("Helena");
            avroMessage.setName("Forklift");
            producer.send(avroMessage);
        }
        final Consumer c = new Consumer(ForkliftAvroConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
            assertTrue("producer property missing on message", isPropsSet);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Queue("forklift-avro-topic")
    public static class ForkliftAvroConsumer {

        @forklift.decorators.Message
        private ComplexAvroMessage value;

        @forklift.decorators.Properties
        private Map<String, String> properties;

        @Producer(queue = "forklift-avro-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value.getName() + value.getAddress().getCity());
            isInjectNull = injectedProducer != null ? false : true;
            isPropsSet = properties.get("Eye").equals("producerProperty");
        }
    }
}
