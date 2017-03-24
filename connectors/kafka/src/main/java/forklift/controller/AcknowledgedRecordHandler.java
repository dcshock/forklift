package forklift.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains a batch of acknowledged records and provides management for adding and removing {@link org.apache.kafka.common.TopicPartition partitions}.
 * Acknowledged records are those that have started processing but have not yet been committed to the Kafka Broker.  This class is threadsafe.
 */
public class AcknowledgedRecordHandler {
    private static final Logger log = LoggerFactory.getLogger(AcknowledgedRecordHandler.class);
    private final Object pausedLock = new Object();
    private final Object unpausedLock = new Object();
    private final AtomicInteger acknowledgeEntryCount = new AtomicInteger(0);
    private ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();
    private volatile boolean acknowledgementsPaused = false;
    private Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();

    /**
     * Acknowledges that a record has been received before processing begins.  True is returned if processing should occur else false.
     * Only records belonging to {@link #addPartitions(java.util.Collection) added partitions} may be processed. Note that this is a
     * blocking method and a short delay may occur should the available topic paritions be changing.
     *
     * @param record the record to acknowledge
     * @return true if the record has been acknowledged and may be processed, else false
     * @throws InterruptedException if interrupted
     */
    public boolean acknowledgeRecord(ConsumerRecord<?, ?> record) throws InterruptedException {
        boolean acknowledged;
        synchronized (unpausedLock) {
            while (acknowledgementsPaused) {
                unpausedLock.wait();
            }
            acknowledgeEntryCount.incrementAndGet();
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        if (!assignment.contains(topicPartition)) {
            acknowledged = false;
        } else {
            long commitOffset = record.offset() + 1;
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(commitOffset, "Commit From Forklift Server");
            pendingOffsets.merge(topicPartition, offsetAndMetadata, this::greaterOffset);
            acknowledged = true;
        }
        synchronized (pausedLock) {
            int count = acknowledgeEntryCount.decrementAndGet();
            if (acknowledgementsPaused && count == 0) {
                pausedLock.notifyAll();
            }
        }
        return acknowledged;
    }

    private OffsetAndMetadata greaterOffset(OffsetAndMetadata a, OffsetAndMetadata b) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        return a.offset() > b.offset() ? a : b;
    }

    /**
     * Removes and returns the highest offsets of any acknowledged records.
     * This is a blocking method as a short delay may occur while any threads which are currently acknowledging records are allowed to
     * complete and any incoming threads are paused.
     *
     * @return a Map of the highest offset data for any acknowledged records
     * @throws InterruptedException if interrupted
     */
    public Map<TopicPartition, OffsetAndMetadata> flushAcknowledged() throws InterruptedException {
        try {
            this.pauseAcknowledgments();
            Map<TopicPartition, OffsetAndMetadata> flushed = pendingOffsets;
            pendingOffsets = new ConcurrentHashMap<>();
            return flushed;
        } finally {
            this.unpauseAcknowledgements();
        }
    }

    /**
     * Adds additional partitions to be managed.  Only added partitions can be
     * {@link #acknowledgeRecord(org.apache.kafka.clients.consumer.ConsumerRecord) acknowledged}
     *
     * @param addedPartitions the partitions to add
     */
    public void addPartitions(Collection<TopicPartition> addedPartitions) {
        this.assignment.addAll(addedPartitions);
    }

    /**
     * Remove partitions from management.  Any existing offsets for the removed partitions are returned.  Note that the offset is the highest
     * acknowledged message's offset + 1 per kafka's specification of how to commit offsets.  Note that this
     * is a blocking method as any threads which are currently acknowledging records are allowed to complete and any
     * incoming threads are paused.
     *
     * @param removedPartitions the partitions to remove
     * @return the highest offsets of the removed partitions
     * @throws InterruptedException if interrupted
     */
    public Map<TopicPartition, OffsetAndMetadata> removePartitions(Collection<TopicPartition> removedPartitions)
                    throws InterruptedException {
        pauseAcknowledgments();
        try {
            Map<TopicPartition, OffsetAndMetadata> removedOffsets = new HashMap<>();
            for (TopicPartition topicPartition : removedPartitions) {
                if (pendingOffsets.containsKey(topicPartition)) {
                    removedOffsets.put(topicPartition, pendingOffsets.remove(topicPartition));
                }
            }
            assignment.removeAll(removedPartitions);
            return removedOffsets;
        } catch (Exception e) {
            log.info("Error in removePartitions", e);
            throw e;
        } finally {
            unpauseAcknowledgements();
        }
    }

    private void pauseAcknowledgments() throws InterruptedException {
        synchronized (pausedLock) {
            acknowledgementsPaused = true;
            while (acknowledgeEntryCount.get() != 0) {
                pausedLock.wait();
            }
        }
    }

    private void unpauseAcknowledgements() {
        synchronized (unpausedLock) {
            acknowledgementsPaused = false;
            unpausedLock.notifyAll();
        }
    }
}
