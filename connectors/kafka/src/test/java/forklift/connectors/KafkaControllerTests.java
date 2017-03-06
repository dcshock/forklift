package forklift.connectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by afrieze on 3/3/17.
 */
public class KafkaControllerTests {

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
        this.controller = new KafkaController(kafkaConsumer, messageStream);
    }

    @After
    public void teardown() throws InterruptedException {
        this.controller.stop(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void addTopicTrueTest() {
        String topic1 = "topic1";
        boolean added = this.controller.addTopic(topic1);
        assertEquals(true, added);
    }

    @Test
    public void addTopicFalseTest() {
        String topic1 = "topic1";
        boolean added = this.controller.addTopic(topic1);
        assertEquals(true, added);
        added = this.controller.addTopic(topic1);
        assertEquals(false, added);
    }

    @Test
    public void removeAddedTopicTest() {
        String topic1 = "topic1";
        this.controller.addTopic(topic1);
        boolean removed = this.controller.removeTopic(topic1);
        assertEquals(true, removed);
    }

    @Test
    public void removeNotAddedTopicTest() {
        String topic1 = "topic1";
        boolean removed = this.controller.removeTopic(topic1);
        assertEquals(false, removed);
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
        String topic1 = "topic1";
        ConsumerRecords records = mock(ConsumerRecords.class);
        when(kafkaConsumer.poll(anyLong())).thenReturn(records);
        this.controller.start();
        this.controller.addTopic(topic1);
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        //verify that the control loop is polling repeatedly.  Normally there would be a delay but
        //the kafkaConsumer has been mocked to return immediatly on poll
        verify(kafkaConsumer, timeout(200).atLeast(5)).poll((anyLong()));
        assertEquals(1, subscribeCaptor.getValue().size());
        assertTrue(subscribeCaptor.getValue().contains(topic1));
        this.controller.stop(10, TimeUnit.MILLISECONDS);
    }

    /**
     * Tests that a topic can be added and subscribed, then removed and unsubscribed, then added again and the controller
     * still functions
     */
    @Test
    public void addRemoveAddTest() throws InterruptedException {
        String topic1 = "topic1";
        ConsumerRecords records = mock(ConsumerRecords.class);
        when(kafkaConsumer.poll(anyLong())).thenReturn(records);
        this.controller.start();
        this.controller.addTopic(topic1);
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        //verify that the control loop is polling repeatedly.  Normally there would be a delay but
        //the kafkaConsumer has been mocked to return immediatly on poll
        verify(kafkaConsumer, timeout(200).atLeast(5)).poll((anyLong()));
        assertEquals(1, subscribeCaptor.getValue().size());
        assertTrue(subscribeCaptor.getValue().contains(topic1));
        //remove the topic
        this.controller.removeTopic(topic1);
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        assertEquals(0, subscribeCaptor.getValue().size());
        this.controller.stop(10, TimeUnit.MILLISECONDS);
        //add the topic back
        this.controller.addTopic(topic1);
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        verify(kafkaConsumer, timeout(200).atLeast(5)).poll((anyLong()));
        assertEquals(1, subscribeCaptor.getValue().size());
        assertTrue(subscribeCaptor.getValue().contains(topic1));
    }

    /**
     * Tests that the controller stops running if an error is encountered putting messages on the stream
     *
     * @throws InterruptedException
     */
    @Test
    public void shutdownOnMessageStreamError() throws InterruptedException {
        String topic1 = "topic1";
        ConsumerRecord record1 = generateRecord(topic1, 1,"value1", 1);
        Map<TopicPartition, List<ConsumerRecord>> recordMap = new HashMap<>();
        List<ConsumerRecord> topicRecords = new ArrayList<>();
        topicRecords.add(record1);
        recordMap.put(new TopicPartition(topic1, 1), topicRecords);
        ConsumerRecords records = new ConsumerRecords(recordMap);
        when(kafkaConsumer.poll(anyLong())).thenReturn(records);
        doThrow(new IllegalStateException()).when(messageStream).addRecords(any());
        this.controller.start();
        this.controller.addTopic(topic1);
        verify(kafkaConsumer, timeout(200).times(1)).subscribe(subscribeCaptor.capture(), any());
        Thread.sleep(50);
        assertEquals(false, controller.isRunning());
    }

    private ConsumerRecord<?, ?> generateRecord(String topic, int partition, String value, long offset) {
        return new ConsumerRecord<Object, Object>(topic, partition, offset, null, value);
    }

}
