package forklift.consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import forklift.message.KafkaMessage;
import org.junit.Before;
import org.junit.Test;

public class KafkaTopicConsumerTests {

    private String topic;
    private KafkaController controller;
    private MessageStream messageStream;
    private KafkaTopicConsumer consumer;

    @Before
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

    @Test(expected = ConnectorException.class)
    public void receiveWithControllerNotRunning() throws ConnectorException {
        when(this.controller.isRunning()).thenReturn(false);
        consumer.receive(100);
    }

    @Test
    public void addTopicTest() throws ConnectorException {
        consumer.receive(100);
        verify(controller).addTopic(this.topic);
    }

    @Test
    public void closeAndRemoveTopicTest() throws ConnectorException {
        consumer.close();
        verify(controller).removeTopic(this.topic);
    }
}
