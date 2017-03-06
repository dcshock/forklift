package forklift.connectors;

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
    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();
    private Object pausedLock = new Object();
    private Object unpausedLock = new Object();
    private AtomicInteger acknowledgeEntryCount = new AtomicInteger(0);
    private volatile boolean acknowledgementsPaused = false;
    private Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();

    /**
     * Acknowledges that a record has been received before processing begins.  True is returned if processing should occur else false.
     * Only records belonging to {@link #addPartitions(java.util.Collection) added partitions} may be processed. Note that this is a
     * blocking method and a short delay may occur should the available topic paritions be changing.
     *
     * @param record
     * @return true if the record has been achnowledged and may be processed, else false
     * @throws InterruptedException
     */
    public boolean acknowledgeRecord(ConsumerRecord<?, ?> record) throws InterruptedException {
        boolean acknowledged = false;
        synchronized (unpausedLock) {
            while (acknowledgementsPaused) {
                unpausedLock.wait();
            }
            acknowledgeEntryCount.incrementAndGet();
        }
        if (!assignment.contains(new TopicPartition(record.topic(), record.partition()))) {
            acknowledged = false;
        } else {
            long offset = record.offset() + 1;
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            synchronized(this) {
                if (!pendingOffsets.containsKey(topicPartition) || pendingOffsets.get(topicPartition).offset() < offset) {
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, "Commit From Forklift Server");
                    pendingOffsets.put(topicPartition, offsetAndMetadata);
                }
            }
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
        } catch (InterruptedException interrupt) {
            Thread.currentThread().interrupt();
            throw interrupt;
        } catch (Throwable e) {
            log.error("Error flushing Acknowledged", e);
            throw e;
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
     * Remove partitions from management.  Any existing offsets for the removed partitions are returned.  Note that the offest is the highest
     * acknowleged message's offset + 1 per kafka's specification of how to commit offsets.  Note that this
     * is a blocking method as any threads which are currently acknowledging records are allowed to complete and any
     * incoming threads are paused.
     *
     * @param removedPartitions the partitions to remove
     * @return the highest offsets of the removed partitions
     * @throws InterruptedException
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
        synchronized (unpausedLock){
            acknowledgementsPaused = false;
            unpausedLock.notifyAll();
        }
    }

}
