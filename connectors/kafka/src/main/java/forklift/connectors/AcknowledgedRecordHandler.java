package forklift.connectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by afrieze on 3/3/17.
 */
public class AcknowledgedRecordHandler {
    private Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();
    private AtomicInteger acknowledgeEntryCount = new AtomicInteger(0);
    private CountDownLatch pauseLatch = new CountDownLatch(0);
    private CountDownLatch unpauseLatch = new CountDownLatch(0);
    private volatile boolean acknowledgementsPaused = false;
    private Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();

    public boolean acknowledgeRecord(ConsumerRecord<?, ?> record) throws InterruptedException {
        boolean acknowledged = false;
        synchronized (this) {
            if (acknowledgementsPaused) {
                unpauseLatch.await();
            }
            acknowledgeEntryCount.incrementAndGet();
        }
        if (!assignment.contains(new TopicPartition(record.topic(), record.partition()))) {
            acknowledged = false;
        } else {
            long offset = record.offset() + 1;
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            if (!pendingOffsets.containsKey(topicPartition) || pendingOffsets.get(topicPartition).offset() < offset) {
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, "Commit From Forklift Server");
                pendingOffsets.put(topicPartition, offsetAndMetadata);
            }
            acknowledged = true;
        }

        if (acknowledgementsPaused) {
            synchronized (this) {
                if (acknowledgeEntryCount.decrementAndGet() == 0) {
                    pauseLatch.await();
                }
            }
        } else {
            acknowledgeEntryCount.decrementAndGet();
        }
        return acknowledged;
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> flushPending() throws InterruptedException {
        this.pauseAcknowledgments();
        Map<TopicPartition, OffsetAndMetadata> flushed = pendingOffsets;
        pendingOffsets = new ConcurrentHashMap<>();
        this.unpauseAcknowledgements();
        return pendingOffsets;
    }

    public void addPartitions(Collection<TopicPartition> addedPartitions) {
        this.assignment.addAll(addedPartitions);
    }

    public synchronized Map<TopicPartition, OffsetAndMetadata> removePartitions(Collection<TopicPartition> removedPartitions)
                    throws InterruptedException {
        pauseAcknowledgments();
        Map<TopicPartition, OffsetAndMetadata> removedOffsets = new HashMap<>();
        for (TopicPartition topicPartition : removedPartitions) {
            if (pendingOffsets.containsKey(topicPartition)) {
                removedOffsets.put(topicPartition, pendingOffsets.remove(topicPartition));
            }
        }
        unpauseAcknowledgements();
        return removedOffsets;
    }

    private synchronized void pauseAcknowledgments() throws InterruptedException {
        if(acknowledgeEntryCount.get() > 0) {
            pauseLatch = new CountDownLatch(1);
        }
        unpauseLatch = new CountDownLatch(1);
        acknowledgementsPaused = true;
        pauseLatch.await();
    }

    private synchronized void unpauseAcknowledgements() {
        pauseLatch.countDown();
        acknowledgementsPaused = false;
    }

}
