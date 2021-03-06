package forklift.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Created by afrieze on 3/3/17.
 */
public class KafkaControllerTests {

    String topic1 = "topic1";
    @Mock
    private MessageStream messageStream;

    @Mock
    private KafkaConsumer<?, ?> kafkaConsumer;

    private KafkaController controller;

    @Captor
    private ArgumentCaptor<Collection<String>> subscribeCaptor;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.controller = new KafkaController(kafkaConsumer, messageStream, topic1);
    }

    @AfterEach
    public void teardown() throws InterruptedException {
        this.controller.stop(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void isRunningTest() throws InterruptedException {
        this.controller.start();
        assertEquals(true, controller.isRunning());
        this.controller.stop(10, TimeUnit.MILLISECONDS);
        assertEquals(false, controller.isRunning());
    }

    @Test
    public void firstSubscribeAndPollingTest() throws InterruptedException {
        ConsumerRecords records = mock(ConsumerRecords.class);
        when(kafkaConsumer.poll(anyLong())).thenReturn(records);
        this.controller.start();
        verify(kafkaConsumer, timeout(200).times(1))
            .subscribe(subscribeCaptor.capture(), any());
        //verify that the control loop is polling repeatedly.  Normally there would be a delay but
        //the kafkaConsumer has been mocked to return immediatly on poll
        verify(kafkaConsumer, timeout(200).atLeast(5))
            .poll(eq(KafkaController.POLL_TIMEOUT));
        assertEquals(1, subscribeCaptor.getValue().size());
        assertTrue(subscribeCaptor.getValue().contains(topic1));
        this.controller.stop(10, TimeUnit.MILLISECONDS);
    }
}
