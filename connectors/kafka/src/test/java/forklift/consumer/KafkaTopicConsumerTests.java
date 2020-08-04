package forklift.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import forklift.message.KafkaMessage;

public class KafkaTopicConsumerTests {

    private String topic;
    private KafkaController controller;
    private MessageStream messageStream;
    private KafkaTopicConsumer consumer;

    @BeforeAll
    public void setup() {
        this.topic = "testTopic";
        this.controller = mock(KafkaController.class);
        when(controller.isRunning()).thenReturn(true);
        this.messageStream = mock(MessageStream.class);
        when(controller.getMessageStream()).thenReturn(this.messageStream);
        consumer = new KafkaTopicConsumer(topic, controller);
    }

    @Test
    public void receiveTimeoutTest() throws InterruptedException, ConnectorException {
        long timeout = 100;
        when(messageStream.nextRecord(this.topic, timeout)).thenReturn(null);
        ForkliftMessage result = consumer.receive(timeout);
        assertEquals(null, result);
    }

    @Test
    public void receiveMessageTest() throws InterruptedException, ConnectorException {
        KafkaMessage message = mock(KafkaMessage.class);
        long timeout = 100;
        when(messageStream.nextRecord(this.topic, timeout)).thenReturn(message);
        ForkliftMessage result = consumer.receive(timeout);
        assertEquals(message, result);
    }

    @Test
    public void receiveWithControllerNotRunning() throws ConnectorException {
        when(this.controller.isRunning()).thenReturn(false);
        assertThrows(ConnectorException.class, () -> {
            consumer.receive(100);
        });
    }
}
