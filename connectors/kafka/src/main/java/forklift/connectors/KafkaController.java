package forklift.connectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;

/**
 * Created by afrieze on 3/1/17.
 */
public class KafkaController {

    private volatile boolean running = false;
    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final KafkaConsumer<?, ?> kafkaConsumer;
    private final RecordStream recordStream;
    private volatile boolean topicsChanged = false;
    private Object topicsMonitor = new Object();
    private AcknowledgedRecordHandler acknowlegmentHandler = new AcknowledgedRecordHandler();

    public KafkaController(KafkaConsumer<?, ?> kafkaConsumer, RecordStream recordStream) {
        this.kafkaConsumer = kafkaConsumer;
        this.recordStream = recordStream;
    }

    public void addTopic(String topic) {
        if (!topics.contains(topic)) {
            recordStream.addTopic(topic);
            topics.add(topic);
            topicsChanged = true;
            synchronized (topicsMonitor) {
                topicsMonitor.notify();
            }
        }
    }

    public void removeTopic(String topic) {
        topics.remove(topic);
        recordStream.removeTopic(topic);
        topicsChanged = true;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean acknowledge(ConsumerRecord<?, ?> record) throws InterruptedException, JMSException {
        log.info("Acknowledge message");
        return running && this.acknowlegmentHandler.acknowledgeRecord(record);
    }

    public void start() {
        running = true;
        executor.submit(() -> controlLoop());
    }

    public void stop(long timeout, TimeUnit timeUnit) throws InterruptedException {
        running = false;
        kafkaConsumer.wakeup();
        executor.shutdownNow();
        executor.awaitTermination(timeout, timeUnit);
    }

    private void controlLoop() {
        try {
            while (running) {
                boolean updatedAssignment = false;
                if (topics.size() == 0) {
                    if (kafkaConsumer.assignment().size() > 0) {
                        kafkaConsumer.unsubscribe();
                    } else {
                        synchronized (topicsMonitor) {
                            //recheck wait condition inside synchronized block
                            if (topics.size() == 0) {
                                //pause the polling thread until a topic comes in
                                topicsMonitor.wait();
                            }
                        }
                    }
                }
                if (topicsChanged) {
                    topicsChanged = false;
                    kafkaConsumer.subscribe(topics, new RebalanceListener());
                    updatedAssignment = true;
                }
                ConsumerRecords<?, ?> records = kafkaConsumer.poll(100);
                if (updatedAssignment) {
                    this.acknowlegmentHandler.addPartitions(kafkaConsumer.assignment());
                }
                if (records.count() > 0) {
                    log.info("Adding: " + records.count() + " to record stream");
                    recordStream.addRecords(records, this);
                }
                Map<TopicPartition, OffsetAndMetadata> offsetData = this.acknowlegmentHandler.flushPending();
                if (offsetData.size() > 0) {
                    kafkaConsumer.commitSync(offsetData);
                }
            }
        } catch (WakeupException | InterruptedException e) {
            log.info("Control Loop exiting");
        } catch (Throwable t) {
            log.error("Control Loop error, exiting", t);
            throw t;
        } finally {
            running = false;
            //the kafkaConsumer must be closed in the poll thread
            kafkaConsumer.close();
        }
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.debug("Partitions revoked");
            try {
                Map<TopicPartition, OffsetAndMetadata>
                                removedOffsetData =
                                acknowlegmentHandler.removePartitions(partitions);

                kafkaConsumer.commitSync(removedOffsetData);
            } catch (InterruptedException e) {
                log.info("Partition rebalance interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("partitions assigned");
            acknowlegmentHandler.addPartitions(partitions);
        }

    }
}
