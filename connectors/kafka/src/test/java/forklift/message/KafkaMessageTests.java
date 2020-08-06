package forklift.message;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import forklift.connectors.ConnectorException;
import forklift.controller.KafkaController;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaMessageTests {

    private static KafkaController controller;
    private static KafkaMessage message;
    private static ConsumerRecord<?, ?> record;

    @BeforeEach
    public void setup() {
        controller = mock(KafkaController.class);
        record = new ConsumerRecord<>("testTopic", 0, 1L, "key", "value");
        message = new KafkaMessage(controller, record, 0);
    }

    @Test
    public void beforeProcessingFalseTest() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(eq(record), any(Long.class))).thenReturn(false);

        boolean acknowledged = message.beforeProcessing();

        assertFalse(acknowledged);
        verify(controller).acknowledge(eq(record), any(Long.class));
    }

    @Test
    public void beforeProcessingCallsControllerSuccess() throws ConnectorException, InterruptedException {
        when(controller.acknowledge(eq(record), any(Long.class))).thenReturn(true);

        boolean acknowledged = message.beforeProcessing();

        assertTrue(acknowledged);
        verify(controller).acknowledge(eq(record), any(Long.class));
    }
}
