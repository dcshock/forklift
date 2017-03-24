package forklift.integration;

import static org.junit.Assert.assertTrue;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
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
public class ForkliftMessageTests {

    private static final Logger log = LoggerFactory.getLogger(ForkliftMessageTests.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;
    //    private static boolean ordered = true;
    private static boolean isPropsSet = false;
    //    private static boolean isHeadersSet = false;
    private static boolean isPropOverwritten = true;


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
    public void testForkliftMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        ForkliftConnectorI connector = serviceManager.newManagedForkliftInstance().getConnector();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        connector.getQueueProducer("forklift-string-topic");
        HashMap<String, String> properties = new HashMap<>();
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("Eye", "overwriteme");
        producer.setProperties(producerProps);
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            Map<String, String> props = new HashMap<>();
            props.put("Eye", "" + i);
            props.put("Foo", "Bar");
            ForkliftMessage forkliftMessage = new ForkliftMessage(msg);
            forkliftMessage.setProperties(props);
            producer.send(forkliftMessage);
        }
        final Consumer c = new Consumer(ForkliftMessageConsumer.class, connector);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue("Message properties were overwritten", isPropOverwritten == false);
            assertTrue("Message properties were not set", isPropsSet == true);
            assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });
        // Start the consumer.
        c.listen();
        assertTrue(called.get() == msgCount);
    }

    @Queue("forklift-string-topic")
    public static class ForkliftMessageConsumer {

        @forklift.decorators.Message
        private ForkliftMessage message;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (message == null || message.getMsg() == null) {
                return;
            }
            int i = called.getAndIncrement();
            System.out.println(Thread.currentThread().getName() + message.getMsg());
            isInjectNull = injectedProducer != null ? false : true;
            //make sure the message property is not overwritten by the producer property
            isPropOverwritten = message.getProperties().get("Eye").equals("overwriteme") ? true : false;
            isPropsSet = message.getProperties().get("Foo").equals("Bar") ? true : false;
        }
    }
}
