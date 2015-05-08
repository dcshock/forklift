package forklift.producers;

import forklift.connectors.ForkliftMessage;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;

public interface ForkliftProducerI {
    void close() throws ProducerException;
    void send(ForkliftMessage message) throws ProducerException;
    //void send(ForkliftMessage message, AsyncCallback onComplete) throws ProducerException;
    //void send(Destination destination, ForkliftMessage message, AsyncCallback onComplete) throws ProducerException;
    
    ProducerInfoI getProducerInfo();
    void setProducerInfo(ProducerInfoI info);
}  