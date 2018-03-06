package forklift.controller;

import forklift.message.MessageStream;
import forklift.schemas.UserRegistered;
import forklift.util.ConsumerRecordCreator;
import forklift.util.TestConsumer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaControllerRebalanceTests {
    private static final String TOPIC = "user.controller.test";
    private AtomicBoolean donePolling;
    private TestConsumer<String, GenericRecord> consumer;
    private MessageStream messageStream;
    private KafkaController controller;

    @Before
    public void setup() {
        donePolling = new AtomicBoolean(false);
        consumer = new TestConsumer<>(3, 3);
        messageStream = new MessageStream();
        controller = new KafkaController(consumer, messageStream, TOPIC);
    }

    private void waitForIt() {
        while (!donePolling.get()) {
            synchronized (donePolling) {
                try {
                    donePolling.wait();
                } catch (InterruptedException e) {}
            }
        }
    }

    @Test
    public void testRecordsAfterRebalance() throws Exception {
        final TopicPartition partition = new TopicPartition(TOPIC, 0);
        final LinkedHashMap<String, GenericRecord> records = new LinkedHashMap<String, GenericRecord>() {{
            put("Bobby", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Bobby");
                put("lastName", "Foo");
                put("email", "bfoo@yahoo.com");
            }});
            put("Billy", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Billy");
                put("lastName", "Joseph");
                put("email", "bjoseph@aol.com");
            }});
            put("Dude", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Dude");
                put("lastName", "Duderson");
                put("email", "dduderson@gmail.com");
            }});
        }};

        consumer.setPartitionsForTopic(TOPIC, new HashSet<>(Arrays.asList(partition)));
        consumer.setPartitionRecords(partition, ConsumerRecordCreator.from(partition, records));
        consumer.runOnceAfterPoll(() -> consumer.rebalancePartitions(Arrays.asList(partition)));
        consumer.runOnceAfterPoll(() -> consumer.rebalancePartitions(Arrays.asList(partition)));
        consumer.runOnceAfterPoll(() -> consumer.rebalancePartitions(Arrays.asList(partition)));
        consumer.runOnceAfterPoll(() -> consumer.rebalancePartitions(Arrays.asList(partition)));
        consumer.runOnceAfterPoll(() -> consumer.rebalancePartitions(Arrays.asList(partition)));
        consumer.runOnceAfterPoll(() -> {});
        consumer.runOnceAfterPoll(() -> {
            synchronized (donePolling) {
                donePolling.set(true);
                donePolling.notifyAll();
            }
        });

        controller.start();
        waitForIt();
        controller.stop(10, TimeUnit.MILLISECONDS);

        Assert.assertEquals(3, messageStream.getQueueForTopic(TOPIC).size());
    }
}
