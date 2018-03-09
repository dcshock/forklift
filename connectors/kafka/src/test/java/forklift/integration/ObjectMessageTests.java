package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.consumer.Consumer;
import forklift.exception.StartupException;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectMessageTests extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RebalanceTests.class);

    
    public void testSendObjectMessage() throws ConnectorException, ProducerException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        int msgCount = 100;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-object-topic");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);
            sentMessageIds.add(producer.send(m));
        }

        final Consumer c = new Consumer(ForkliftObjectConsumer.class, forklift);
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
