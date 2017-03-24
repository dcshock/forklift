package forklift.integration;

import static org.junit.Assert.assertTrue;
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
 * Created by afrieze on 3/14/17.
 */
public class MapMessageTests {

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

    private static final Logger log = LoggerFactory.getLogger(MapMessageTests.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;

    @Test
    public void testSendMapValueMessage() throws ConnectorException, ProducerException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-map-topic");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            producer.send(m);
        }

        final Consumer c = new Consumer(ForkliftMapConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Queue("forklift-map-topic")
    public static class ForkliftMapConsumer {

        @forklift.decorators.Message
        private Map<String, String> mapMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (mapMessage == null || mapMessage.size() == 0) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + mapMessage);
            isInjectNull = injectedProducer != null ? false : true;
        }
    }
}
