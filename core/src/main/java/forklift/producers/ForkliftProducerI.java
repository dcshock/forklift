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
    String send(Map<Header,String> headers, 
                Map<String,String> props,
                ForkliftMessage message) throws ProducerException;
    void close() throws ProducerException;
    Map<Header, String> getHeaders() throws ProducerException;
    void setHeaders(Map<Header, String> headers) throws ProducerException;
    Map<String, String> getProps() throws ProducerException;
    void setProps(Map<String , String> props) throws ProducerException;
}  