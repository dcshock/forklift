package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import java.util.List;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;

public interface ForkliftProducerI {
    String send(String message) throws ProducerException;
    String send(ForkliftMessage message) throws ProducerException;
    String send(Map<Header, String> headers, 
                Map<String, Object> properties,
                ForkliftMessage message) throws ProducerException;
    Map<Header, String> getHeaders() throws ProducerException;
    void setHeaders(Map<Header, String> headers) throws ProducerException;
    Map<String, Object> getProperties() throws ProducerException;
    void setProperties(Map<String , Object> properties) throws ProducerException;
}  