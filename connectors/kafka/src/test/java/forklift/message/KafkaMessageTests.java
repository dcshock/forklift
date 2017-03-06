package forklift.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import forklift.connectors.KafkaController;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import javax.jms.JMSException;

public class KafkaMessageTests {

    private KafkaController controller;
    private KafkaMessage message;
    private ConsumerRecord record;

    @Before
    public void setup() {
        controller = mock(KafkaController.class);
        record = new ConsumerRecord("testTopic", 0, 1L, "key", "value");
        message = new KafkaMessage(controller, record);
    }

    @Test
    public void acknowledgeJMSRebalanceException() throws InterruptedException {
        boolean exception = false;
        try {
            when(controller.acknowledge(record)).thenReturn(false);
            message.acknowledge();
        } catch (JMSException e) {
            exception = true;
            assertEquals("KAFKA-REBALANCE", e.getErrorCode());
        }
        assertTrue(exception);
    }

    @Test
    public void acknowledgeCallsControllerSuccess() throws JMSException, InterruptedException {
        when(controller.acknowledge(record)).thenReturn(true);
        message.acknowledge();
        verify(controller).acknowledge(record);
    }
}
