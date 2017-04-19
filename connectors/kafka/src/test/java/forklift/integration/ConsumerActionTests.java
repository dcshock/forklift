package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.consumer.Consumer;
import forklift.exception.StartupException;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.junit.Test;

public class ConsumerActionTests extends BaseIntegrationTest {

    /**
     * Test subscribing to a topic, unsubscribing, then resubscribing to the topic.
     */
    @Test
    public void topicTurnoverTest() throws StartupException, ConnectorException, InterruptedException, ProducerException {

        String topic = "forklift-string-topic";
        Forklift forklift = serviceManager.newManagedForkliftInstance();
        ForkliftProducerI producer = forklift.getConnector().getQueueProducer("forklift-string-topic");
        sentMessageIds.add(producer.send("message1"));
        final Consumer c1 = new Consumer(StringConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c1.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        // Start the consumer.
        c1.listen();
        Thread.sleep(3000);
        sentMessageIds.add(producer.send("message2"));
        final Consumer c2 = new Consumer(StringConsumer.class, forklift);
        // Shutdown the consumer after all the messages have been processed.
        c2.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        // Start the consumer.
        c2.listen();
        messageAsserts();
    }

}
