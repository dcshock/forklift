package forklift.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.ConsumerThread;
import forklift.consumer.KafkaTopicConsumer;
import forklift.decorators.Message;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.message.KafkaMessage;
import forklift.message.MessageStream;
import forklift.schemas.UserRegistered;
import forklift.source.decorators.Topic;
import forklift.util.ConsumerRecordCreator;
import forklift.util.TestConsumer;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

public class KafkaControllerRebalanceTests {
    private static final String TOPIC = "user.controller.test";
    private static List<ConsumerRecord<?, ?>> processedRecords;

    private AtomicBoolean donePolling;
    private TestConsumer<String, GenericRecord> kafkaConsumer;
    private MessageStream messageStream;
    private KafkaController controller;

    private Consumer consumer;
    private ConsumerThread consumerThread;
    private KafkaTopicConsumer forkliftConsumer;
    private ForkliftConnectorI mockConnector;
    private Forklift forklift;

    @Before
    public void setup() throws Exception {
        donePolling = new AtomicBoolean(false);
        processedRecords = new ArrayList<>();

        kafkaConsumer = new TestConsumer<>(2, 2);
        messageStream = new MessageStream();
        controller = spy(new KafkaController(kafkaConsumer, messageStream, TOPIC));

        forkliftConsumer = new KafkaTopicConsumer(TOPIC, controller);
        mockConnector = mock(ForkliftConnectorI.class);
        when(mockConnector.getConsumerForSource(any())).thenReturn(forkliftConsumer);

        forklift = new Forklift();
        forklift.setConnector(mockConnector);

        consumer = new Consumer(LoggingConsumer.class, forklift);
        consumer.setOutOfMessages(consumer -> {
            synchronized (donePolling) {
                donePolling.set(true);
                donePolling.notifyAll();
            }
        });
        consumerThread = new ConsumerThread(consumer);
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
    @SuppressWarnings("unchecked")
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
            put("Bobby2", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Bobby2");
                put("lastName", "Foo");
                put("email", "bfoo@yahoo.com");
            }});
            put("Billy2", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Billy2");
                put("lastName", "Joseph");
                put("email", "bjoseph@aol.com");
            }});
            put("Dude2", new GenericData.Record(UserRegistered.getClassSchema()) {{
                put("firstName", "Dude2");
                put("lastName", "Duderson");
                put("email", "dduderson@gmail.com");
            }});
        }};
        final List<ConsumerRecord<String, GenericRecord>> recordList = ConsumerRecordCreator.from(partition, records);
        final List<TopicPartition> partitions = Arrays.asList(partition);

        kafkaConsumer.setPartitionsForTopic(TOPIC, new HashSet<>(partitions));
        kafkaConsumer.setPartitionRecords(partition, recordList);

        kafkaConsumer.queueRebalance(partitions);
        kafkaConsumer.queueRebalance(partitions);
        kafkaConsumer.queueRebalance(partitions);
        kafkaConsumer.runOnceBeforePoll(() -> {});
        kafkaConsumer.queueRebalance(partitions);
        kafkaConsumer.queueRebalance(partitions);

        controller.start();
        consumerThread.start();

        waitForIt();

        consumerThread.shutdown();
        controller.stop(10, TimeUnit.MILLISECONDS);

        // since the consumer has one thread, the messages should be processed in order
        Assert.assertEquals(recordList.size(), processedRecords.size());
        Assert.assertEquals(recordList, processedRecords);
    }

    @Topic("ignored")
    @MultiThreaded(1)
    public static class LoggingConsumer {
        private ForkliftMessage message;

        @Inject
        public LoggingConsumer(@Message ForkliftMessage message) {
            this.message = message;
        }

        @OnMessage
        public void onMessage() {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}

            processedRecords.add(((KafkaMessage) message).getConsumerRecord());
            System.out.println("Processed message: " + message.getMsg());
        }
    }
}
