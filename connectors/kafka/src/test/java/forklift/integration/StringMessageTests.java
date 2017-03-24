package forklift.integration;

import static org.junit.Assert.assertTrue;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.decorators.MultiThreaded;
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
 * Created by afrieze on 3/14/17.
 */
public class StringMessageTests {

    private static final Logger log = LoggerFactory.getLogger(StringMessageTests.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;

    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        called.set(0);
    }

    @Test
    public void testStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-string-topic");
        HashMap<String, Object> properties = new HashMap<>();
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            Map<String, Object> props = new HashMap<>();
            props.put("Foo", "bar");
            props.put("Eye", "" + i);
            producer.send(msg);
        }
        final Consumer c = new Consumer(StringConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Test
    public void testMultiThreadedStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 100;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }
        final Consumer
                        c =
                        new Consumer(MultiThreadedStringConsumer.class,
                                     connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Queue("forklift-string-topic")
    public static class StringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @MultiThreaded(10)
    @Queue("forklift-string-topic")
    public static class MultiThreadedStringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + value);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }
}
