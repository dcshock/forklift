package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.producers.ProducerInfoI;

import org.apache.activemq.ActiveMQMessageProducer;

public class ActiveMQProducer implements ForkliftProducerI {

    private ProducerInfoI info;
    private javax.jms.MessageProducer producer;

    public ActiveMQProducer() {
    
    }

    public ActiveMQProducer(ProducerInfoI producerInfo) {
        this.info = producerInfo;
    }

    public ActiveMQProducer(javax.jms.MessageProducer producer) {
        this.producer = producer;
    }

    @Override
    public void send(ForkliftMessage message) throws ProducerException {
        // TODO: write send code
        try {
            producer.send(message.getJmsMsg());
        } catch (Exception e) {
            throw new ProducerException("Failed to send message");
        }
        
    }

    @Override
    public void close() throws ProducerException {
        // TODO: write code to close the producer
        try {
            producer.close();
        } catch (Exception e) {
            throw new ProducerException("Failed to close producer");
        }
        
    }

    @Override
    public ProducerInfoI getProducerInfo() {
        return this.info;
    }

    @Override
    public void setProducerInfo(ProducerInfoI info) {
        this.info = info;
    }
}
