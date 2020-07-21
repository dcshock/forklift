package forklift.controller;

import forklift.message.KafkaMessage;
import forklift.message.MessageStream;
import forklift.message.ReadableMessageStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 * are batched and performed on any {@link KafkaController#acknowledge(ConsumerRecord, long) acknowledged records}.
 * <p>
 * <strong>WARNING: </strong>Kafka does not lend itself well to message level commits.  For this reason, the controller sends commits
 * as a batch once every poll cycle.  It should be noted that it is possible for a message to be
 * processed twice should an error occur after the acknowledgement and processing of a message but before the commit takes place.
 */
public class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);
    private volatile boolean running = false;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Consumer<?, ?> kafkaConsumer;
    private final MessageStream messageStream;
    private final String topic;
    private AcknowledgedRecordHandler acknowledgmentHandler = new AcknowledgedRecordHandler();
    private Map<TopicPartition, Generation> generations = new ConcurrentHashMap<>();
    private Map<TopicPartition, OffsetAndMetadata> failedOffset = null;
    private Map<TopicPartition, AtomicInteger> flowControl = new ConcurrentHashMap<>();

    public KafkaController(Consumer<?, ?> kafkaConsumer, MessageStream messageStream, String topic) {
        this.kafkaConsumer = kafkaConsumer;
        this.messageStream = messageStream;
        messageStream.addTopic(topic);
        this.topic = topic;
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
     * @param generationNumber the generation number when the record was received
     * @return true if the record should be processed, else false
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean acknowledge(ConsumerRecord<?, ?> record, long generationNumber) throws InterruptedException {
        final TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        final AtomicInteger flowCount = flowControl.get(partition);
        if (flowCount != null) {
            int counter = flowCount.decrementAndGet();
            if (counter == 0) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        final Generation generation = generations.get(partition);

        log.debug("Acknowledge message with topic {} partition {} offset {}", record.topic(), record.partition(), record.offset());
        return running && acknowledgmentHandler.acknowledgeRecord(
            record,
            () -> generation.acceptsGeneration(generationNumber));
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
        running = false;
        kafkaConsumer.wakeup();
        executor.shutdown();
        if (!executor.awaitTermination(timeout, timeUnit)) {
            log.error("Failed to stop KafkaController in {} {}", timeout, timeUnit);
        }
    }

    private void controlLoop() {
        try {
            kafkaConsumer.subscribe(Collections.singleton(topic), new RebalanceListener());
            while (running) {
                ConsumerRecords<?, ?> records = flowControlledPoll();
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
                //Heartbeat is sent on poll. This call should not return records.
                return kafkaConsumer.poll(0);
            }
        } else {
            return kafkaConsumer.poll(100);
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
            final TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            final Generation generation = generations.get(partition);

            messages.get(partition).add(new KafkaMessage(this, record, generation.generationNumber()));
        }
        return messages;
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("controlLoop partitions revoked with generations {}", generations);
            try {
                for (TopicPartition partition : partitions) {
                    generations.get(partition).unassign();
                }

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

            for (TopicPartition partition : partitions) {
                final Generation generation = generations.computeIfAbsent(partition, ignored -> new Generation());
                generation.assignNextGeneration();
            }

            log.debug("controlLoop partitions assigned with generations {}", generations);

            acknowledgmentHandler.addPartitions(partitions);
            for (TopicPartition partition : partitions) {
                flowControl.merge(partition, new AtomicInteger(),
                                  (oldValue, newValue) -> oldValue == null ? newValue : oldValue);
            }
        }

    }
}
