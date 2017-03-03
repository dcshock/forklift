package forklift.consumer;

import forklift.connectors.KafkaController;
import forklift.connectors.RecordStream;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Created by afrieze on 2/27/17.
 */
public class KafkaTopicConsumer implements ForkliftConsumerI {
    private final String topic;
    private final KafkaController controller;
    private final RecordStream recordStream;

    public KafkaTopicConsumer(String topic, KafkaController controller, RecordStream recordStream) {
        this.topic = topic;
        this.controller = controller;
        this.recordStream = recordStream;
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        try {
            controller.addTopic(topic);
            return recordStream.nextRecord(this.topic, timeout);
        } catch (InterruptedException e) {
            throw new JMSException("Kafka message receive interrupted");
        }
    }

    @Override
    public void close() throws JMSException {
        controller.removeTopic(topic);
    }
}
