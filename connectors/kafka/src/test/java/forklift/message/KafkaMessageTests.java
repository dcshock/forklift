package forklift.message;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import forklift.connectors.ConnectorException;
import forklift.controller.KafkaController;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

public class KafkaMessageTests {

    private KafkaController controller;
    private KafkaMessage message;
    private ConsumerRecord record;

    @Before
    public void setup() {
        controller = mock(KafkaController.class);
        record = new ConsumerRecord("testTopic", 0, 1L, "key", "value");
        message = new KafkaMessage(controller, record, 0);
    }

    
    public void acknowledgeFalseTest() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(record, any())).thenReturn(false);

        boolean acknowledged = message.acknowledge();

        assertFalse(acknowledged);
        verify(controller).acknowledge(record, any());
    }

    
    public void acknowledgeCallsControllerSuccess() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(record, any())).thenReturn(true);

        boolean acknowledged = message.acknowledge();

        assertTrue(acknowledged);
        verify(controller).acknowledge(record, any());
    }
}
