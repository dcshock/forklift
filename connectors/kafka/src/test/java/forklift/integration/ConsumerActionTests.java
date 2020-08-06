package forklift.integration;

import org.junit.jupiter.api.Test;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.consumer.Consumer;
import forklift.exception.StartupException;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;

public class ConsumerActionTests extends BaseIntegrationTest {

    /**
     * Test subscribing to a topic, unsubscribing, then resubscribing to the topic.
     */
    @Test
    public void topicTurnoverTest() throws StartupException, ConnectorException, InterruptedException, ProducerException {
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        ForkliftProducerI producer = forklift.getConnector().getQueueProducer("forklift-string-topic");
        sentMessageIds.add(producer.send("message1"));
        final Consumer c1 = new Consumer(StringConsumer.class, forklift);

        // Shutdown the consumer after all the messages have been processed.
        c1.setOutOfMessages((listener) -> {
            timeouts++;

            if (sentMessageIds.equals(consumedMessageIds) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        // Start the consumer.
        c1.listen();

        sentMessageIds.add(producer.send("message2"));
        final Consumer c2 = new Consumer(StringConsumer.class, forklift);

        // Shutdown the consumer after all the messages have been processed.
        timeouts = 0;
        c2.setOutOfMessages((listener) -> {
            timeouts++;

            if (sentMessageIds.equals(consumedMessageIds) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        // Start the consumer.
        c2.listen();
        messageAsserts();
    }

}
