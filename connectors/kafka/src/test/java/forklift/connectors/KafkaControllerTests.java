package forklift.connectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import forklift.controller.KafkaController;
import forklift.message.MessageStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.controller = new KafkaController(kafkaConsumer, messageStream, topic1);
    }

    @After
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
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        //verify that the control loop is polling repeatedly.  Normally there would be a delay but
        //the kafkaConsumer has been mocked to return immediatly on poll
        verify(kafkaConsumer, timeout(200).atLeast(5)).poll((anyLong()));
        assertEquals(1, subscribeCaptor.getValue().size());
        assertTrue(subscribeCaptor.getValue().contains(topic1));
        this.controller.stop(10, TimeUnit.MILLISECONDS);
    }

    private ConsumerRecord<?, ?> generateRecord(String topic, int partition, String value, long offset) {
        return new ConsumerRecord<Object, Object>(topic, partition, offset, null, value);
    }

}
