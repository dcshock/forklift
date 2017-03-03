package forklift.connectors;

import forklift.message.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.jms.Message;

/**
 * Created by afrieze on 3/1/17.
 */
public class RecordStream {
    private static final Logger log = LoggerFactory.getLogger(RecordStream.class);
    Map<String, BlockingQueue<Message>> topicQueue = new ConcurrentHashMap<>();

    public void addRecords(ConsumerRecords<?, ?> records, KafkaController controller) {

        try {
            log.info("Adding records to stream");
            for (TopicPartition partition : records.partitions()) {
                if (!topicQueue.containsKey(partition.topic())) {
                    topicQueue.put(partition.topic(), new LinkedBlockingQueue<>());
                }
            }
            for (ConsumerRecord<?, ?> record : records) {
                topicQueue.get(record.topic()).add(new KafkaMessage(controller, record));
            }
        } catch(Exception e){
            log.error("Error adding to stream", e);
        }
    }

    public void addTopic(String topic) {
        this.topicQueue.put(topic, new LinkedBlockingQueue<>());
    }

    public void removeTopic(String topic) {
        this.topicQueue.remove(topic);
    }

    public Message nextRecord(String topic, long timeout) throws InterruptedException {
        Message message = topicQueue.get(topic).poll(timeout, TimeUnit.MILLISECONDS);
        log.info("Returning: " + (message == null?"null":"message") + " for nextRecord call");
        return message;
    }

}
