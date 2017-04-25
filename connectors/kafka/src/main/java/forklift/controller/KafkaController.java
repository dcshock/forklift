package forklift.controller;

import forklift.message.KafkaMessage;
import forklift.message.MessageStream;
import forklift.message.ReadableMessageStream;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

/**
 * Manages the {@link org.apache.kafka.clients.consumer.KafkaConsumer} thread.  Polled records are sent to the MessageStream. Commits
 * are batched and performed on any {@link #acknowledge(org.apache.kafka.clients.consumer.ConsumerRecord) acknowledged records}.
 * <p>
 * <strong>WARNING: </strong>Kafka does not lend itself well to message level commits.  For this reason, the controller sends commits
 * as a batch once every poll cycle.  It should be noted that it is possible for a message to be
 * processed twice should an error occur after the acknowledgement and processing of a message but before the commit takes place.
 */
public class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);
    private volatile boolean running = false;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaConsumer<?, ?> kafkaConsumer;
    private final MessageStream messageStream;
    private AcknowledgedRecordHandler acknowledgmentHandler = new AcknowledgedRecordHandler();
    private Map<TopicPartition, OffsetAndMetadata> failedOffset = null;
    private Map<TopicPartition, AtomicInteger> flowControl = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private String topicToAdd;
    @GuardedBy("this")
    private String topicToRemove;

    public KafkaController(KafkaConsumer<?, ?> kafkaConsumer, MessageStream messageStream) {
        this.kafkaConsumer = kafkaConsumer;
        this.messageStream = messageStream;
    }

    /**
     * Returns a {@link forklift.message.ReadableMessageStream} which is fed by messages polled by this controller.
     *
     * @return a {@link forklift.message.ReadableMessageStream}
     */
    public ReadableMessageStream getMessageStream() {
        return this.messageStream;
    }

    /**
     * Adds a topic which the underlying {@link org.apache.kafka.clients.consumer.KafkaConsumer} will be subscribed to.  Adds
     * the topic to the messageStream.
     *
     * @param topic the topic to subscribe to
     * @return true if the topic was added, false if already added
     */
    public synchronized boolean addTopic(String topic) throws InterruptedException {
        while (topicToAdd != null) {
            this.wait();
        }
        if (!topics.contains(topic)) {
            this.topicToAdd = topic;
            //Notify that topics have changed
            this.notifyAll();
            while (this.topicToAdd != null && running) {
                //wait for the control loop to add the topic
                this.wait();
            }
            return true;
        }
        return false;
    }

    /**
     * Unsubscribe the underlying {@link org.apache.kafka.clients.consumer.KafkaConsumer} from the passed in topic and removes the
     * topic from the message stream.
     *
     * @param topic the topic to remove
     * @return true if the topic was removed, false if it wasn't present
     */
    public synchronized boolean removeTopic(String topic) throws InterruptedException {
        while (topicToRemove != null) {
            this.wait();
        }
        if (topics.contains(topic)) {
            this.topicToRemove = topic;
            //Notify that topics have changed
            this.notifyAll();
            while (this.topicToRemove != null && running) {
                //wait for the control loop to remove the topic
                this.wait();
            }
            return true;
        }
        return false;
    }

    /**
     * Whether or not this service is running.
     *
     * @return true if this service is running, else false
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Acknowledge that processing is beginning on a record.  True is returned if this controller is still managing the
     * partition the record originated from.  If the partition is no longer owned by this controller, false is returned
     * and the message should not be processed.
     *
     * @param record the record to acknowledge
     * @return true if the record should be processed, else false
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean acknowledge(ConsumerRecord<?, ?> record) throws InterruptedException {
        AtomicInteger flowCount = flowControl.get(new TopicPartition(record.topic(), record.partition()));
        if (flowCount != null) {
            int counter = flowCount.decrementAndGet();
            if (counter == 0) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
        log.debug("Acknowledge message with topic {} partition {} offset {}", record.topic(), record.partition(), record.offset());
        return running && acknowledgmentHandler.acknowledgeRecord(record);
    }

    /**
     * Starts the controller.  Must be called before any other methods.
     * <p>
     * <strong>WARNING: </strong>Avoid starting a service which has already run and been stopped.  No attempt is made
     * to recover a stopped controller.
     */
    public void start() {
        running = true;
        executor.submit(this::controlLoop);
    }

    /**
     * Stops the controller.
     *
     * @param timeout  the time to wait for the service to stop
     * @param timeUnit the TimeUnit of timeout
     * @throws InterruptedException if the thread is interrupted
     */
    public void stop(long timeout, TimeUnit timeUnit) throws InterruptedException {
        kafkaConsumer.wakeup();
        executor.shutdownNow();

        running = false;
        if (!executor.awaitTermination(timeout, timeUnit)) {
            log.error("Failed to stop KafkaController in {} {}", timeout, timeUnit);
        }
    }

    private void controlLoop() {
        try {
            while (running) {
                boolean updatedAssignment;
                synchronized (this) {
                    updatedAssignment = processTopicChanges();
                    //notify any threads waiting on processing of the topic changes
                    this.notifyAll();
                    if (topics.size() == 0) {
                        waitForTopicToAdd();
                        continue;
                    }
                }
                ConsumerRecords<?, ?> records = pollForRecords(updatedAssignment);
                //Update the assignment before adding records to stream
                if (updatedAssignment) {
                    updateAssignment();
                }
                addRecordsToStream(records);
                Map<TopicPartition, OffsetAndMetadata> offsets = acknowledgmentHandler.getAcknowledged();
                commitOffsets(offsets);
            }
        } catch (WakeupException e) {
            log.info("Wakeup, controlLoop exiting");
        } catch (InterruptedException e) {
            log.info("Interrupted, controlLoop exiting");
        } catch (Throwable t) {
            log.error("controlLoop error, exiting", t);
            throw t;
        } finally {
            running = false;
            synchronized (this) {
                //wake up any threads waiting on a control loop operation
                this.notifyAll();
            }
            try {
                Map<TopicPartition, OffsetAndMetadata> finalOffsets = new HashMap<>();
                if (failedOffset != null) {
                    finalOffsets.putAll(failedOffset);
                }
                finalOffsets.putAll(acknowledgmentHandler.removePartitions(kafkaConsumer.assignment()));
                try {
                    log.info("closing offset size committed: " + finalOffsets.size());
                    try {
                        //if we got here through running = false or interrupt instead of a wakeup, we need
                        //to retry our commit as the first attempt will throw a WakeupException
                        kafkaConsumer.commitSync(finalOffsets);
                    } catch (WakeupException expected) {
                        log.info("controlLoop wakeup on closing commitSync, retrying");
                        kafkaConsumer.commitSync(finalOffsets);
                    }
                } catch (Throwable e) {
                    log.error("controlLoop error commiting sync", e);
                }
            } catch (InterruptedException e) {
                log.info("controlLoop failed to commit offsets on shutdown", e);
            }
            //the kafkaConsumer must be closed in the poll thread
            log.info("controlLoop closing kafkaConsumer");
            kafkaConsumer.close();
        }
    }

    private boolean processTopicChanges() throws InterruptedException {
        boolean topicsAdded = processTopicAdd();
        boolean topicsRemoved = processTopicRemoved();
        boolean topicsChanged = topicsAdded || topicsRemoved;

        if (topicsChanged) {
            kafkaConsumer.subscribe(topics, new RebalanceListener());
        }
        return topicsChanged;
    }

    private boolean processTopicAdd() {
        if (topicToAdd != null) {
            topics.add(topicToAdd);
            messageStream.addTopic(topicToAdd);
            topicToAdd = null;
            return true;
        }
        return false;
    }

    private boolean processTopicRemoved() throws InterruptedException {
        if (topicToRemove != null) {
            topics.remove(topicToRemove);
            messageStream.removeTopic(topicToRemove);
            Set<TopicPartition> removed = kafkaConsumer.assignment().stream().filter(partition -> !topics.contains(partition.topic())).collect(Collectors.toSet());
            Map<TopicPartition, OffsetAndMetadata> offsets = acknowledgmentHandler.removePartitions(removed);
            removed.forEach(partition -> flowControl.remove(partition));
            commitOffsets(offsets);
            topicToRemove = null;
            return true;
        }
        return false;
    }

    private void waitForTopicToAdd() throws InterruptedException {
        synchronized (this) {
            //recheck wait condition inside synchronized block
            while (topicToAdd == null) {
                //pause the polling thread until a topic comes in
                log.debug("controlLoop waiting for topic");
                this.wait();
            }
        }
    }

    private ConsumerRecords<?, ?> pollForRecords(boolean updatedAssignment) throws InterruptedException {
        /**
         * if we have an updated assignment, we cannot do a flow controlled poll as the flowControl collection has not been
         * updated and cannot be updated until the kafkaConsumer.assignment is updated through a call to kafkaConsumer.poll
         */
        if (updatedAssignment) {
            return kafkaConsumer.poll(100);
        }
        return flowControlledPoll();
    }

    private ConsumerRecords<?, ?> flowControlledPoll() throws InterruptedException {
        //pause partitions that haven't fully been processed yet and unpause those that have
        Set<TopicPartition> paused = new HashSet<>();
        Set<TopicPartition> unpaused = new HashSet<>();
        for (Map.Entry<TopicPartition, AtomicInteger> entry : flowControl.entrySet()) {
            if (entry.getValue().get() > 0) {
                paused.add(entry.getKey());
            } else {
                unpaused.add(entry.getKey());
            }
        }
        kafkaConsumer.pause(paused);
        kafkaConsumer.resume(unpaused);

        if (unpaused.size() == 0 && paused.size() > 0) {
            synchronized (this) {
                //wait for flowControl to notify us, resume after a short pause to allow for heartbeats
                this.wait(100);
            }
            return null;
        } else {
            return kafkaConsumer.poll(100);
        }
    }

    private void updateAssignment() {
        acknowledgmentHandler.addPartitions(kafkaConsumer.assignment());
        for (TopicPartition partition : kafkaConsumer.assignment()) {
            this.flowControl.merge(partition, new AtomicInteger(),
                                   (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
        }
    }

    private void addRecordsToStream(ConsumerRecords<?, ?> records) {
        if (records != null && records.count() > 0) {
            log.debug("adding: " + records.count() + " to record stream");
            records.forEach(record -> flowControl.get(new TopicPartition(record.topic(), record.partition()))
                                                 .incrementAndGet());
            messageStream.addRecords(consumerRecordsToKafkaMessages(records));
        }
    }

    private void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.size() > 0) {
            //if the commit fails, perhaps due to a wakeup, store the offsets so they can be retried before the controller stops
            failedOffset = offsets;
            kafkaConsumer.commitSync(offsets);
            failedOffset = null;
        }
    }

    private Map<TopicPartition, List<KafkaMessage>> consumerRecordsToKafkaMessages(ConsumerRecords<?, ?> records) {
        Map<TopicPartition, List<KafkaMessage>> messages = new HashMap<>();
        records.partitions().forEach(tp -> messages.put(tp, new ArrayList<>()));
        for (ConsumerRecord<?, ?> record : records) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            messages.get(partition).add(new KafkaMessage(this, record));
        }
        return messages;
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("controlLoop partitions revoked");
            try {
                Map<TopicPartition, OffsetAndMetadata> removedOffsets = acknowledgmentHandler.removePartitions(partitions);
                for (TopicPartition partition : partitions) {
                    flowControl.remove(partition);
                }

                kafkaConsumer.commitSync(removedOffsets);
            } catch (InterruptedException e) {
                log.debug("controlLoop partition rebalance interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("controlLoop partitions assigned");
            acknowledgmentHandler.addPartitions(partitions);
            for (TopicPartition partition : partitions) {
                flowControl.merge(partition, new AtomicInteger(),
                                  (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
            }
        }

    }
}
