package forklift.connectors;

import static org.junit.Assert.assertEquals;
import forklift.controller.AcknowledgedRecordHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by afrieze on 3/3/17.
 */
public class AcknowledgedRecordHandlerTests {

    private AcknowledgedRecordHandler handler;
    private Supplier<Boolean> predicate;

    @Before
    public void setup() {
        this.handler = new AcknowledgedRecordHandler();
        this.predicate = () -> true;
    }

    
    public void acknowledgeRecordFalseTest() throws InterruptedException{
        ConsumerRecord<?,?> record = generateRecord("topic1", 0, "value1", 0);
        boolean acknowledged = this.handler.acknowledgeRecord(record, predicate);
        assertEquals(false, acknowledged);
    }

    
    public void acknowledgeRecordTrueTest() throws InterruptedException{
        int partition = 0;
        String topic1 = "topic1";
        ConsumerRecord<?,?> record = generateRecord(topic1, partition, "value1", 0);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition(topic1, partition));
        this.handler.addPartitions(partitions);
        boolean acknowledged = this.handler.acknowledgeRecord(record, predicate);
        assertEquals(true, acknowledged);
    }

    
    public void removePartitionsOffsetTest() throws InterruptedException{
        int partition = 0;
        long offset = 123;
        String topic1 = "topic1";
        ConsumerRecord<?,?> record = generateRecord(topic1, partition, "value1", offset);
        TopicPartition topicPartition = new TopicPartition(topic1, partition);
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        this.handler.addPartitions(partitions);
        boolean acknowledged = this.handler.acknowledgeRecord(record, predicate);
        Map<TopicPartition, OffsetAndMetadata> offsets = this.handler.removePartitions(partitions);
        assertEquals(true, offsets.containsKey(topicPartition));
        assertEquals(offset+1, offsets.get(topicPartition).offset());
    }


    
    public void removePartitionAcknowledgeTest() throws InterruptedException{
        int partition = 0;
        long offset = 123;
        String topic1 = "topic1";
        ConsumerRecord<?,?> record = generateRecord(topic1, partition, "value1", offset);
        TopicPartition topicPartition = new TopicPartition(topic1, partition);
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        this.handler.addPartitions(partitions);
        Map<TopicPartition, OffsetAndMetadata> offsets = this.handler.removePartitions(partitions);
        boolean acknowledged = this.handler.acknowledgeRecord(record, predicate);
        assertEquals(false, acknowledged);
    }


    @Test
    public void falsePredicateFailsAckTest() throws Exception {
        // set up for success
        int partition = 0;
        String topic1 = "topic1";
        ConsumerRecord<?,?> record = generateRecord(topic1, partition, "value1", 0);
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition(topic1, partition));
        this.handler.addPartitions(partitions);

        // but have the predicate fail
        boolean acknowledged = this.handler.acknowledgeRecord(record, () -> false);
        assertEquals(false, acknowledged);
    }

    private ConsumerRecord<?, ?> generateRecord(String topic, int partition, String value, long offset) {
        return new ConsumerRecord<Object, Object>(topic, partition, offset, null, value);
    }
}
