package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;

import org.apache.activemq.ActiveMQMessageProducer;

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.IllegalAccessException;
import java.lang.NoSuchFieldException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class ActiveMQProducer implements ForkliftProducerI {

    private static final Logger log = LoggerFactory.getLogger(ActiveMQProducer.class);
    private MessageProducer producer;
    private Destination destination;
    private Map<Header, String> headers;
    private Map<String, String> props;
    private Session session;

    public ActiveMQProducer(MessageProducer producer, Session session) {
        this.producer = producer;
        this.session = session;
    }

    @Override
    public String send(String message) throws ProducerException {
        try {
            ForkliftMessage fork = new ForkliftMessage();
            fork.setMsg(message);
            Message msg = prepAndValidate(fork);
            producer.send(msg);
            return msg.getJMSCorrelationID();
        } catch (Exception e) {
            throw new ProducerException("Failed to send message");
        }
    }

    @Override
    public String send(ForkliftMessage message) throws ProducerException {
        try {
            Message msg = prepAndValidate(message);
            producer.send(msg);
            return msg.getJMSCorrelationID();
        } catch (Exception e) {
            throw new ProducerException("Failed to send message");
        }
    }

    @Override
    public String send(Map<Header, String> headers, 
                       Map<String,String> props,
                       ForkliftMessage message) throws ProducerException {
        
        Message msg = prepAndValidate(message);

        // set headers.
        try {
            setMessageHeaders(msg, headers);
        } catch (Exception e) {
            throw new ProducerException("Failed to set message header: " + e);
        }

        // set props
        try {
            for(Map.Entry<String, String> prop : props.entrySet()) {
                msg.setStringProperty(prop.getKey(), prop.getValue());
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to set message property: " + e);
        }

        try {
            producer.send(msg);
            return msg.getJMSCorrelationID();
        } catch (Exception e) {
            throw new ProducerException("Failed to send message");
        }
    }

    /**
    * Generate javax.jms.Message from a Forklift Message
    * @param message - Forklift message to be converted
    * @return - a new javax.jms.Message
    **/
    private Message forkliftToJms(ForkliftMessage message) throws ProducerException {
        try {
            return message.getJmsMsg() != null ?  message.getJmsMsg() : session.createTextMessage(message.getMsg());
        } catch (Exception e) {
            throw new ProducerException("Error creating JMS Message, Forklift message was null");
        }
    }

    /**
    * validate message and prepare for sending.
    * @param msg - ForkliftMessage to be checked
    * @return - jms message ready to be sent to endpoint
    **/
    private Message prepAndValidate(ForkliftMessage message) throws ProducerException {
        Message msg = forkliftToJms(message);

        // check the ID
        try {
            if (Strings.isNullOrEmpty(msg.getJMSCorrelationID())) {
                msg.setJMSCorrelationID(UUID.randomUUID().toString().replaceAll("-", "")); 
            }
        } catch (Exception e) {
            throw new ProducerException("Failed to set setJMSCorrelationID");
        }

        return msg;
    }

    private void setMessageHeaders(Message msg, Map<Header, String> headers) throws ProducerException {
        for (Map.Entry<Header,String> header : headers.entrySet()) {
            Method method;
            try {
                method = msg.getClass().getMethod("set"+header.getKey().getJmsMessage() , String.class);
                method.invoke(msg, header.getValue());
            } catch (Exception e) {
                throw new ProducerException("Failed to set" + header.getKey().getJmsMessage() + "(" + header.getValue() + ") message header");
            }
        }
    }

    @Override
    public void close() throws ProducerException {
        try {
            producer.close();
        } catch (Exception e) {
            throw new ProducerException("Failed to close producer");
        }
    }


    public int getDeliveryMode() throws ProducerException {
        try {
            return this.producer.getDeliveryMode();
        } catch (Exception e) {
            throw new ProducerException("Failed to get DelieveryMode");
        }
    }

    public void setDeliveryMode(int mode) throws ProducerException {
        try {
            this.producer.setDeliveryMode(mode);
        } catch (Exception e) {
            throw new ProducerException("Failed to set DelieveryMode");
        }
    }

    public long getTimeToLive() throws ProducerException {
        try {
            return this.producer.getTimeToLive();
        } catch (Exception e) {
            throw new ProducerException("Failed to get TimeToLive");
        }
    }

    public void setTimeToLive(long timeToLive) throws ProducerException {
        try {
            this.producer.setTimeToLive(timeToLive);
        } catch (Exception e) {
            throw new ProducerException("Failed to set TimeToLive");
        }   
    }

    @Override
    public Map<Header, String> getHeaders() throws ProducerException {
        try {
            return this.headers;
        } catch (Exception e) {
            throw new ProducerException("Failed to get headers");   
        }
        
    }

    @Override
    public void setHeaders(Map<Header, String> headers) throws ProducerException {
        try {
            this.headers = headers;
        } catch (Exception e) {
            throw new ProducerException("Failed to set headers");
        }
    }

    @Override
    public Map<String, String> getProps() throws ProducerException {
        try {
            return this.props;
        } catch (Exception e) {
            throw new ProducerException("Failed to get props");
        }
    }

    @Override
    public void setProps(Map<String , String> props) throws ProducerException {
        try {
            this.props = props;
        } catch (Exception e) {
            throw new ProducerException("Failed to set props");
        }
    }
}
