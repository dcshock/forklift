package forklift.integration;

import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.schemas.StateCode;
import forklift.schemas.UserRegistered;
import forklift.source.decorators.Queue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AvroMessageTests extends BaseIntegrationTest {
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
    public void testComplexAvroMessageWithProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-avro-topic");
        Map<String, String> producerProps = new HashMap<>();
        producerProps.put("Eye", "producerProperty");
        producer.setProperties(producerProps);
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
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
    public void testComplexAvroMessageWithoutProperty() throws ProducerException, ConnectorException, InterruptedException, StartupException {
        Forklift forklift = serviceManager.newManagedForkliftInstance("");
        int msgCount = 10;
        ForkliftProducerI
                        producer =
                        forklift.getConnector().getQueueProducer("forklift-avro-topic");
        for (int i = 0; i < msgCount; i++) {
            UserRegistered registered = new UserRegistered();
            registered.setFirstName("John");
            registered.setLastName("Doe");
            registered.setEmail("test@test.com");
            registered.setState(StateCode.MT);
            sentMessageIds.add(producer.send(registered));
        }
        final Consumer c = new Consumer(AvroMessageTests.RegisteredAvroConsumer.class, forklift);
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

    @Queue("forklift-avro-topic")
    public static class RegisteredAvroConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private UserRegistered value;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            System.out.println(Thread.currentThread().getName() + value.getState());
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }
}
