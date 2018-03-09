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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Maintains a batch of acknowledged records and provides management for adding and removing {@link org.apache.kafka.common.TopicPartition partitions}.
 * Acknowledged records are those that have started processing but have not yet been committed to the Kafka Broker.  This class is threadsafe.
 */
public class AcknowledgedRecordHandler {
    private static final Logger log = LoggerFactory.getLogger(AcknowledgedRecordHandler.class);
    private ConcurrentHashMap<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
    private Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();
    //A ReentrantReadWriteLock is used here to allow for multiple threads to access the acknowledgeRecord method
    //concurrently with the read lock, but then pause all entry with the write lock
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Acknowledges that a record has been received before processing begins.
     * Only records belonging to {@link #addPartitions(java.util.Collection) added partitions} may be processed. Note that this is a
     * blocking method and a short delay may occur should the available topic paritions be changing.
     *
     * @param record the record to acknowledge
     * @return true if the record has been acknowledged and may be processed, else false
     * @throws InterruptedException if interrupted
     */
    public boolean acknowledgeRecord(ConsumerRecord<?, ?> record) throws InterruptedException {
        lock.readLock().lock();
        try {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            if (!assignment.contains(topicPartition)) {
                return false;
            }

            long commitOffset = record.offset() + 1;
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(commitOffset, "Commit From Forklift Server");
            offsets.merge(topicPartition, offsetAndMetadata, this::greaterOffset);

            System.out.println("Merging commit on partition: " + topicPartition + " offset: " + commitOffset);

            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    private OffsetAndMetadata greaterOffset(OffsetAndMetadata a, OffsetAndMetadata b) {
        return a.offset() > b.offset() ? a : b;
    }

    /**
     * Returns the highest offsets of any acknowledged records.
     *
     * @return a Map of the highest offset data for any acknowledged records
     */
    public Map<TopicPartition, OffsetAndMetadata> getAcknowledged() {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>(offsets);
        return currentOffsets;
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
        lock.writeLock().lock();
        try {
            Map<TopicPartition, OffsetAndMetadata> removedOffsets = new HashMap<>();
            for (TopicPartition topicPartition : removedPartitions) {
                if (offsets.containsKey(topicPartition)) {
                    removedOffsets.put(topicPartition, offsets.remove(topicPartition));
                }
            }
            assignment.removeAll(removedPartitions);
            return removedOffsets;
        } catch (Exception e) {
            log.info("Error in removePartitions", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
