package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.consumer.Consumer;
import forklift.exception.StartupException;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class MapMessageTests extends BaseIntegrationTest {

    @Test
    public void testSendMapValueMessage() throws ConnectorException, ProducerException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-map-topic");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            sentMessageIds.add(producer.send(m));
        }

        final Consumer c = new Consumer(ForkliftMapConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });

        // Start the consumer.
        c.listen();
        messageAsserts();
    }

}
