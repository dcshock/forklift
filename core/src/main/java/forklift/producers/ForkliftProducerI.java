package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;

public interface ForkliftProducerI extends Closeable {
    String send(String message) throws ProducerException;
    String send(ForkliftMessage message) throws ProducerException;
    String send(Object message) throws ProducerException;
    String send(Map<String, String> message) throws ProducerException;
    String send(Map<Header, Object> headers, 
                Map<String, Object> properties,
                ForkliftMessage message) throws ProducerException;
    Map<Header, Object> getHeaders() throws ProducerException;
    void setHeaders(Map<Header, Object> headers) throws ProducerException;
    Map<String, Object> getProperties() throws ProducerException;
    void setProperties(Map<String , Object> properties) throws ProducerException;
}  