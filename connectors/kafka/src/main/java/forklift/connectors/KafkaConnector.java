package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;

public class KafkaConnector implements ForkliftConnectorI {
    private String host;
    private int port;

    public KafkaConnector(String host, int port) {
        this.host = host;
        this.port = port;

    }

    @Override
    public Connection getConnection() throws ConnectorException {
        return null;
    }

    @Override
    public ForkliftConsumerI getQueue(String arg0) throws ConnectorException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ForkliftProducerI getQueueProducer(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ForkliftConsumerI getTopic(String topic) throws ConnectorException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public ForkliftProducerI getTopicProducer(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ForkliftMessage jmsToForklift(Message arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start() throws ConnectorException {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() throws ConnectorException {
        // TODO Auto-generated method stub

    }

}
