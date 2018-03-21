package forklift.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConsumerRecordCreator {
    public static <K, V> List<ConsumerRecord<K, V>> from(TopicPartition partition, LinkedHashMap<K, V> values) {
        final List<ConsumerRecord<K, V>> result = new ArrayList<>();

        int offset = 0;
        for (Map.Entry<K, V> entry : values.entrySet()) {
            result.add(new ConsumerRecord<K, V>(partition.topic(), partition.partition(), offset, entry.getKey(), entry.getValue()));
            offset++;
        }
        return result;
    }
}
