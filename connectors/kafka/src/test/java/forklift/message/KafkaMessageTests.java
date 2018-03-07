package forklift.message;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
    private Generation generation;

    @Before
    public void setup() {
        controller = mock(KafkaController.class);
        record = new ConsumerRecord("testTopic", 0, 1L, "key", "value");
        generation = new Generation();
        generation.assignNextGeneration();

        message = new KafkaMessage(controller, record, generation);
    }

    @Test
    public void acknowledgeFalseTest() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(record)).thenReturn(false);

        boolean acknowledged = message.acknowledge();

        assertFalse(acknowledged);
        verify(controller).acknowledge(record);
    }

    @Test
    public void acknowledgeCallsControllerSuccess() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(record)).thenReturn(true);

        boolean acknowledged = message.acknowledge();

        assertTrue(acknowledged);
        verify(controller).acknowledge(record);
    }

    @Test
    public void acknowledgeFailsWithUnassignedGeneration() throws ConnectorException, InterruptedException {
        generation.unassign();
        when(controller.acknowledge(record)).thenReturn(true);

        boolean acknowledged = message.acknowledge();

        assertFalse(acknowledged);
        verify(controller, never()).acknowledge(record);
    }

    @Test
    public void acknowledgeFailsWithOldGeneration() throws ConnectorException, InterruptedException {
        generation.unassign();
        generation.assignNextGeneration();
        when(controller.acknowledge(record)).thenReturn(true);

        boolean acknowledged = message.acknowledge();

        assertFalse(acknowledged);
        verify(controller, never()).acknowledge(record);
    }
}
