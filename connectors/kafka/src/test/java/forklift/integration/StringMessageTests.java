package forklift.integration;

import static org.junit.Assert.assertTrue;
import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by afrieze on 3/14/17.
 */
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
            listener.shutdown();
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
            listener.shutdown();
        });
        // Start the consumer.
        c.listen();
        messageAsserts();

    }
}
