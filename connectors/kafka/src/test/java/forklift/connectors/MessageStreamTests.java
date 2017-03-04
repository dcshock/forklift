package forklift.connectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import forklift.message.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageStreamTests {

    private KafkaController controller;
    private MessageStream stream;

    @Before
    public void setup() {
        stream = new MessageStream();
    }

    @Test
    public void addAndGetNextMessageTest() throws InterruptedException {
        String topic = "topic1";
        stream.addTopic(topic);
        ConsumerRecord<?, ?> record = generateRecord(topic, "value1");
        Map<TopicPartition, List<KafkaMessage>> messages = generateMessages(record);
        stream.addRecords(messages);
        KafkaMessage message = stream.nextRecord(topic, 100);
        assertEquals(record, message.getConsumerRecord());
    }

    @Test
    public void nextMessageMultipleTopics() throws InterruptedException{

        String topic1 = "topic1";
        String topic2 = "topic2";
        stream.addTopic(topic1);
        stream.addTopic(topic2);
        ConsumerRecord<?, ?> record1 = generateRecord(topic1, "value1");
        ConsumerRecord<?, ?> record2 = generateRecord(topic2, "value1");
        Map<TopicPartition, List<KafkaMessage>> messages = generateMessages(record1, record2);
        stream.addRecords(messages);
        KafkaMessage message1 = stream.nextRecord(topic1, 100);
        KafkaMessage message2 = stream.nextRecord(topic2, 100);
        assertEquals(record1, message1.getConsumerRecord());
        assertEquals(record2, message2.getConsumerRecord());
    }

    @Test
    public void nextMessageNullTest() throws InterruptedException {
        String topic = "topic1";
        stream.addTopic(topic);
        KafkaMessage message = stream.nextRecord(topic, 10);
        assertEquals(null, message);
    }

    @Test
    public void nextMessageOrderTest() throws InterruptedException {
        String topic = "topic1";
        ConsumerRecord[] records = new ConsumerRecord[100];
        for (int i = 0; i < 100; i++) {
            records[i] = generateRecord(topic, "value-" + i);
        }
        Map<TopicPartition, List<KafkaMessage>> messages = generateMessages(records);
        stream.addTopic(topic);
        stream.addRecords(messages);
        for (int i = 0; i < 100; i++) {
            ConsumerRecord record = stream.nextRecord(topic, 10).getConsumerRecord();
            assertEquals("value-" + i, record.value());
        }
    }

    @Test
    public void removeTopic() throws InterruptedException {
        String topic = "topic1";
        stream.addTopic(topic);
        ConsumerRecord<?, ?> record = generateRecord(topic, "value1");
        Map<TopicPartition, List<KafkaMessage>> messages = generateMessages(record);
        stream.addRecords(messages);
        stream.removeTopic(topic);
        KafkaMessage message = stream.nextRecord(topic, 100);
        assertEquals(null, message);
    }

    private ConsumerRecord<?, ?> generateRecord(String topic, String value) {
        return new ConsumerRecord<Object, Object>(topic, 0, 0, null, value);
    }

    private Map<TopicPartition, List<KafkaMessage>> generateMessages(ConsumerRecord... records) {
        Map<TopicPartition, List<KafkaMessage>> messages = new HashMap<>();
        for (ConsumerRecord record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            if (!messages.containsKey(partition)) {
                messages.put(partition, new ArrayList<>());
            }
            KafkaMessage message = mock(KafkaMessage.class);
            when(message.getConsumerRecord()).thenReturn(record);
            messages.get(partition).add(message);
        }
        return messages;
    }

}
