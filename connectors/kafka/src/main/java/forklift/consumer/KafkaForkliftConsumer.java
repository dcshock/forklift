package forklift.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;

public class KafkaForkliftConsumer implements ForkliftConsumerI {
    private KafkaConsumer<String, String> consumer;

    public KafkaForkliftConsumer(String host, int port, String queue) {
        Properties props = new Properties();
        props.put("bootstrap.servers", host + ":" + port);
        props.put("group.id", "forklift-queue");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(queue));
    }

    @Override
    public Message receive(long timeout) throws JMSException {
    }

    @Override
    public void close() throws JMSException {
        // TODO Auto-generated method stub

    }
}
