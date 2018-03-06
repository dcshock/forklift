package forklift.util;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.Collectors;

/**
 A minimal implementation of the {@link Consumer} interface designed to simulate consuming records during a rebalance.
 */
public final class TestConsumer<K, V> implements Consumer<K, V> {
    private final long maxPollRecords;
    private final long maxPollRecordsPerPartition;
    private Map<String, Set<TopicPartition>> partitionsForTopic = new HashMap<>();
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsForPartition = new HashMap<>();
    private Queue<Runnable> beforePollActions = new LinkedBlockingQueue<>();

    private Set<String> subscription= new HashSet<>();
    private Set<TopicPartition> assignment = new HashSet<>();
    private Set<TopicPartition> pausedPartitions = new HashSet<>();
    private ConsumerRebalanceListener listener;

    private Map<TopicPartition, Long> fetchPositions = new HashMap<>();
    private Map<TopicPartition, Long> commitPositions = new HashMap<>();


    public TestConsumer(long maxPollRecords, long maxPollRecordsPerPartition) {
        this.maxPollRecords = maxPollRecords;
        this.maxPollRecordsPerPartition = maxPollRecordsPerPartition;
    }

    /*
       -----
       Subscription and assignment methods
       -----
    */
    @Override public void assign(Collection<TopicPartition> assignment) {
        this.assignment = new HashSet<>(assignment);
    }
    @Override public Set<TopicPartition> assignment() { return assignment; }

    @Override public void subscribe(Collection<String> topics) {
        subscribe(topics, null);
    }
    @Override public void subscribe(java.util.regex.Pattern pattern, ConsumerRebalanceListener listener) {
        final Collection<String> matchingTopics = partitionsForTopic.keySet().stream()
            .filter(topic -> pattern.matcher(topic).matches())
            .collect(Collectors.toList());

        subscribe(matchingTopics, listener);
    }
    @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        this.listener = listener;
        this.subscription = new HashSet<>(topics);

        if (this.listener == null) { this.listener = new NoOpConsumerRebalanceListener(); }

        assignment.clear();
        topics.forEach(this::assignPartitionsForTopic);
        this.listener.onPartitionsAssigned(assignment);
    }
    private void assignPartitionsForTopic(String topic) {
        final Set<TopicPartition> assignedPartitions = partitionsForTopic.getOrDefault(topic, Collections.emptySet());

        assignment.addAll(assignedPartitions);
    }
    @Override public void unsubscribe() {
        assignment.clear();
        subscription.clear();
    }
    @Override public Set<String> subscription() { return subscription; }

    /*
      -----
      Pause and Resume functionality
      -----
    */
    @Override public void pause(Collection<TopicPartition> partitions) {
        pausedPartitions.addAll(partitions);
    }
    @Override public void resume(Collection<TopicPartition> partitions) {
        pausedPartitions.removeAll(partitions);
    }
    @Override public Set<TopicPartition> paused() { return pausedPartitions; }

    /*
      -----
      Position managament functionality
      -----
    */
    @Override public void seek(TopicPartition partition, long offset) {
        fetchPositions.put(partition, offset);
    }
    @Override public void seekToBeginning(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> seek(partition, 0));
    }
    @Override public void seekToEnd(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            final long endOffset = recordsForPartition.get(partition).size();
            seek(partition, endOffset);
        });
    }
    @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return partitions.stream()
            .collect(Collectors.toMap(Function.identity(), partition -> 0L));
    }
    @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return partitions.stream()
            .collect(Collectors.toMap(Function.identity(), partition -> Long.valueOf(recordsForPartition.get(partition).size())));
    }
    @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return timestampsToSearch.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndTimestamp(0L, entry.getValue())));
    }

    @Override public long position(TopicPartition partition) {
        return fetchPositions.getOrDefault(partition, 0L);
    }

    /*
      -----
      Commit functionality
      -----
    */
    @Override public void commitSync() { commitAsync(); }
    @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) { commitAsync(offsets, null); }
    @Override public void commitAsync() { commitAsync(fetchPositionsAndMetadata(), null); }
    @Override public void commitAsync(OffsetCommitCallback callback) { commitAsync(fetchPositionsAndMetadata(), callback); }
    @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        offsets.entrySet().forEach(entry -> {
            final long offset = entry.getValue().offset();

            commitPositions.put(entry.getKey(), offset);
        });
    }

    @Override public OffsetAndMetadata committed(TopicPartition partition) {
        return new OffsetAndMetadata(commitPositions.get(partition));
    }

    private Map<TopicPartition, OffsetAndMetadata> fetchPositionsAndMetadata() {
        return fetchPositions.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new OffsetAndMetadata(entry.getValue())));
    }

    /*
      -----
      Miscellaneous functionality
      -----
    */
    @Override public void close() {}
    @Override public void close(long timeout, java.util.concurrent.TimeUnit unit) {}
    @Override public void wakeup() {}
    @Override public Map<MetricName, ? extends Metric> metrics() { return Collections.emptyMap(); }
    @Override public List<PartitionInfo> partitionsFor(String topic) {
        return listTopics().get(topic);
    }
    @Override public Map<String, List<PartitionInfo>> listTopics() {
        return partitionsForTopic.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                 return entry.getValue().stream()
                     .map(TestConsumer::toDefaultPartitionInfo)
                     .collect(Collectors.toList());
             }));
    }

    private static PartitionInfo toDefaultPartitionInfo(TopicPartition partition) {
        return new PartitionInfo(
            partition.topic(),
            partition.partition(),
            Node.noNode(),
            new Node[0],
            new Node[0]);
    }

    /*
      ----
      Poll functionality
      ----
    */
    public ConsumerRecords<K, V> poll(long timeout) {
        final Runnable action = beforePollActions.poll();
        if (action != null) { action.run(); }

        if (timeout == 0) { return new ConsumerRecords<>(Collections.emptyMap()); }

        final AtomicLong remainingRecordsInBatch = new AtomicLong(maxPollRecords);
        final Map<TopicPartition, List<ConsumerRecord<K, V>>> recordMap = assignment.stream()
            .flatMap(partition -> {
                    if (pausedPartitions.contains(partition)) { return Stream.empty(); }
                    if (remainingRecordsInBatch.get() <= 0) { return Stream.empty(); }

                    final List<ConsumerRecord<K, V>> partitionRecords = recordsForPartition.get(partition);
                    final long fetchPosition = fetchPositions.computeIfAbsent(partition, part -> commitPositions.getOrDefault(part, 0L));
                    final long remainingRecordsInPartition = partitionRecords.size() - fetchPosition;
                    final long numFetchedRecords = Math.min(remainingRecordsInPartition, remainingRecordsInBatch.get());

                    final List<ConsumerRecord<K, V>> fetchedRecords = partitionRecords.subList((int) fetchPosition, (int) (fetchPosition + numFetchedRecords));

                    fetchPositions.put(partition, fetchPosition + numFetchedRecords);
                    remainingRecordsInBatch.addAndGet(-numFetchedRecords);

                    return Stream.of(new AbstractMap.SimpleEntry<TopicPartition, List<ConsumerRecord<K, V>>>(partition, fetchedRecords));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new ConsumerRecords<K, V>(recordMap);
    }

    /*
      -----
      Mock control methods
      -----
    */
    public void setPartitionsForTopic(String topic, Set<TopicPartition> partitions) {
        partitionsForTopic.put(topic, partitions);
    }

    public void setPartitionRecords(TopicPartition partition, List<ConsumerRecord<K, V>> records) {
        recordsForPartition.put(partition, records);
    }

    /*
      -----
      Consumer intraction functionality
      -----
    */

    public void runOnceBeforePoll(Runnable action) {
        beforePollActions.add(action);
    }

    /* Perform a rebalance for the given partitions before fetching records in the next poll */
    public void queueRebalance(Collection<TopicPartition> partitions) {
        runOnceBeforePoll(() -> performRebalance(partitions));
    }

    private void performRebalance(Collection<TopicPartition> partitions) {
        listener.onPartitionsRevoked(partitions);

        partitions.forEach(partition -> {
            fetchPositions.remove(partition);
        });
        resume(partitions);

        listener.onPartitionsAssigned(partitions);
    }
}
