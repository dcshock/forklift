package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.decorators.Queue;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ForkliftMessageTests extends BaseIntegrationTest {

    private static boolean isPropsSet = false;
    private static boolean isPropOverwritten = true;

    @AfterAll
    public void after() {
        serviceManager.stop();
    }

    @BeforeAll
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
    }

    @Test
    public void testForkliftMessageWithProperties()
                    throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-string-topic");
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
            sentMessageIds.add(producer.send(forkliftMessage));
        }
        final Consumer c = new Consumer(ForkliftMessageConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            timeouts++;

            if (sentMessageIds.equals(consumedMessageIds) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        // Start the consumer.
        c.listen();
        messageAsserts();

        assertTrue(isPropOverwritten == false, "Message properties were overwritten");
        assertTrue(isPropsSet == true, "Message properties were not set");
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
            consumedMessageIds.add(message.getId());
            System.out.println(Thread.currentThread().getName() + message.getMsg());
            //make sure the message property is not overwritten by the producer property
            isPropOverwritten = message.getProperties().get("Eye").equals("overwriteme") ? true : false;
            isPropsSet = message.getProperties().get("Foo").equals("Bar") ? true : false;
        }
    }
}
