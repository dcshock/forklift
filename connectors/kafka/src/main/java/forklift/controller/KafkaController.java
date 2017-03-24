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
    private volatile boolean topicsChanged = false;
    private AcknowledgedRecordHandler acknowlegmentHandler = new AcknowledgedRecordHandler();
    private Map<TopicPartition, OffsetAndMetadata> failedOffset = null;
    private Map<TopicPartition, AtomicInteger> flowControl = new ConcurrentHashMap<>();

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
    public synchronized boolean addTopic(String topic) {
        if (!topics.contains(topic)) {
            messageStream.addTopic(topic);
            topics.add(topic);
            topicsChanged = true;
            this.notifyAll();
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
    public synchronized boolean removeTopic(String topic) {
        boolean removed = topics.remove(topic);
        if (removed) {
            messageStream.removeTopic(topic);
            topicsChanged = true;
        }
        return removed;
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
        return running && this.acknowlegmentHandler.acknowledgeRecord(record);
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
        executor.shutdownNow();
        kafkaConsumer.wakeup();
        running = false;
        executor.awaitTermination(timeout, timeUnit);
    }

    private void controlLoop() {
        try {
            while (running) {
                boolean updatedAssignment = false;
                if (topics.size() == 0) {
                    commitPendingAndWaitForTopics();
                }
                if (topicsChanged) {
                    commitAnyPendingOffsetsForRemovedTopics();
                    kafkaConsumer.subscribe(topics, new RebalanceListener());
                    updatedAssignment = true;
                }
                ConsumerRecords<?, ?> records = flowControlledPoll();
                //Must be done before we send records to the acknowledgmentHandler
                if (updatedAssignment) {
                    updateAssignment();
                }
                addRecordsToStream(records);
                Map<TopicPartition, OffsetAndMetadata> offsetData = this.acknowlegmentHandler.flushAcknowledged();
                commitOffsets(offsetData);
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
            try {
                Map<TopicPartition, OffsetAndMetadata>
                                offsetData =
                                this.acknowlegmentHandler.removePartitions(kafkaConsumer.assignment());
                try {
                    if (failedOffset != null) {
                        log.debug("failedOffset of size: " + failedOffset.size());
                        for (TopicPartition partition : failedOffset.keySet()) {
                            offsetData.merge(partition, failedOffset.get(partition), (oldO, newO) -> {
                                if (oldO == null) {
                                    return newO;
                                }
                                if (newO == null) {
                                    return oldO;
                                }
                                return newO.offset() > oldO.offset() ? newO : oldO;
                            });
                        }
                    }

                    log.info("closing offset size committed: " + offsetData.size());
                    try {
                        //if we got here through running = false or interrupt instead of a wakeup, we need
                        //to retry our commit as the first attempt will throw a WakeupException
                        kafkaConsumer.commitSync(offsetData);
                    } catch (WakeupException wakeup) {
                        log.error("controlLoop wakeup on closing commitSync, retrying");
                        kafkaConsumer.commitSync(offsetData);
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

    private void commitPendingAndWaitForTopics() throws InterruptedException {
        //check if the last remaining topic was removed
        if (kafkaConsumer.assignment().size() > 0) {
            Map<TopicPartition, OffsetAndMetadata>
                            offsetData =
                            this.acknowlegmentHandler.removePartitions(kafkaConsumer.assignment());
            failedOffset = offsetData;
            commitOffsets(offsetData);
            kafkaConsumer.unsubscribe();
        }
        synchronized (this) {
            //recheck wait condition inside synchronized block
            while (topics.size() == 0) {
                //pause the polling thread until a topic comes in
                log.debug("controlLoop waiting for topic");
                this.wait();
            }
        }
    }

    private void commitAnyPendingOffsetsForRemovedTopics() throws InterruptedException {
        Set<TopicPartition> removed = new HashSet<>();
        //syncrhonized to avoid letting the topics set change
        synchronized (this) {
            for (TopicPartition partition : kafkaConsumer.assignment()) {
                if (!topics.contains(partition.topic())) {
                    removed.add(partition);
                }
            }
            topicsChanged = false;
        }
        //commit any removed partitions before we unsubscribe them
        Map<TopicPartition, OffsetAndMetadata> offsetData = this.acknowlegmentHandler.removePartitions(removed);
        commitOffsets(offsetData);
    }

    private ConsumerRecords<?,?> flowControlledPoll() throws InterruptedException {
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
                this.wait(100);
            }
            //let the control loop continue so we can unpause partitions or send heartbeats
            return null;
        }
        else {
            return kafkaConsumer.poll(100);
        }
    }

    private void updateAssignment() {
        this.acknowlegmentHandler.addPartitions(kafkaConsumer.assignment());
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
        @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("controlLoop partitions revoked");
            try {
                Map<TopicPartition, OffsetAndMetadata>
                                removedOffsetData =
                                acknowlegmentHandler.removePartitions(partitions);
                for (TopicPartition partition : partitions) {
                    flowControl.remove(partition);
                }

                kafkaConsumer.commitSync(removedOffsetData);
            } catch (InterruptedException e) {
                log.debug("controlLoop partition rebalance interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("controlLoop partitions assigned");
            acknowlegmentHandler.addPartitions(partitions);
            for (TopicPartition partition : partitions) {
                flowControl.merge(partition, new AtomicInteger(),
                                  (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
            }
        }

    }
}
