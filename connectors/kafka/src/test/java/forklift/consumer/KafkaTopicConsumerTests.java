package forklift.consumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import forklift.connectors.KafkaController;
import forklift.connectors.MessageStream;
import forklift.message.KafkaMessage;
import org.junit.Before;
import org.junit.Test;
import javax.jms.JMSException;
import javax.jms.Message;

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
        consumer = new KafkaTopicConsumer(topic, controller, messageStream);
    }

    @Test
    public void receiveTimeoutTest() throws InterruptedException, JMSException {
        long timeout = 100;
        when(messageStream.nextRecord(this.topic, timeout)).thenReturn(null);
        Message result = consumer.receive(timeout);
        assertEquals(null, result);
    }

    @Test
    public void receiveMessageTest() throws InterruptedException, JMSException {
        KafkaMessage message = mock(KafkaMessage.class);
        long timeout = 100;
        when(messageStream.nextRecord(this.topic, timeout)).thenReturn(message);
        Message result = consumer.receive(timeout);
        assertEquals(message, result);
    }

    @Test(expected=JMSException.class)
    public void receiveWithControllerNotRunning() throws JMSException {
        when(this.controller.isRunning()).thenReturn(false);
        consumer.receive(100);
    }

    @Test
    public void addTopicTest() throws JMSException {
        consumer.receive(100);
        verify(controller).addTopic(this.topic);
    }

    @Test
    public void closeAndRemoveTopicTest() throws JMSException {
        consumer.close();
        verify(controller).removeTopic(this.topic);
    }
}
