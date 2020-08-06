package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.consumer.Consumer;
import forklift.exception.StartupException;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class StringMessageTests extends BaseIntegrationTest {

    @Test
    public void testStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        int msgCount = 1000;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-string-topic");
        HashMap<String, Object> properties = new HashMap<>();
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test " + i);
            Map<String, Object> props = new HashMap<>();
            props.put("Foo", "bar");
            props.put("Eye", "" + i);
            sentMessageIds.add(producer.send(msg));
        }
        final Consumer c = new Consumer(StringConsumer.class, forklift);
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
    }

    @Test
    public void testMultiThreadedStringMessage() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        int msgCount = 10000;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-string-topic");
        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test " + i);
            sentMessageIds.add(producer.send(msg));
        }
        final Consumer
                        c =
                        new Consumer(MultiThreadedStringConsumer.class,
                                     forklift);
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

    }
}
