package forklift.connectors;

import forklift.consumer.ActiveMQMessageConsumer;
import forklift.consumer.ForkliftConsumerI;
import forklift.message.ActiveMQHeaders;
import forklift.message.Header;
import forklift.producers.ActiveMQProducer;
import forklift.producers.ForkliftProducerI;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class ActiveMQConnector implements ForkliftConnectorI {
    private Logger log = LoggerFactory.getLogger(ActiveMQConnector.class);

    private ActiveMQConnectionFactory factory;
    private ActiveMQConnection conn;
    private String brokerUrl;

    public ActiveMQConnector() {

    }

    public ActiveMQConnector(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    @Override
    public synchronized void start()
      throws ConnectorException {
        if (brokerUrl == null)
            throw new ConnectorException("brokerUrl wasn't set");

        factory = new ActiveMQConnectionFactory("", "", brokerUrl);
    }

    @Override
    public synchronized void stop()
      throws ConnectorException {
        if (conn != null)
            try {
                conn.close();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }
    }

    @Override
    public synchronized Connection getConnection()
      throws ConnectorException {
        if (conn == null || !conn.isStarted())
            try {
                conn = (ActiveMQConnection)factory.createConnection();
                conn.start();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }

        if (conn == null)
            throw new ConnectorException("Could not create connection to activemq.");

        return conn;
    }

    public synchronized Session getSession()
      throws ConnectorException {
        try {
            return getConnection().createSession(
                false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public ForkliftConsumerI getQueue(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return new ActiveMQMessageConsumer(s.createConsumer(s.createQueue(name)), s);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public ForkliftConsumerI getTopic(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return new ActiveMQMessageConsumer(s.createConsumer(s.createTopic(name)), s);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public ForkliftMessage jmsToForklift(Message m) {
        try {
            final ForkliftMessage msg = new ForkliftMessage(m);
            if (m instanceof ActiveMQTextMessage) {
                msg.setMsg(((ActiveMQTextMessage)m).getText());
            } else {
                msg.setFlagged(true);
                msg.setWarning("Unexpected message type: " + m.getClass().getName());
            }

            Map<Header, Object> headers = new HashMap<>();
            ActiveMQMessage amq = (ActiveMQMessage) m;
            // Build headers
            for (Header h : Header.values()) {
                headers.put(h, ActiveMQHeaders.getFunctions().get(h).get(amq));
            }
            msg.setHeaders(headers);

            // Build properties
            try {
                msg.setProperties(amq.getProperties());
            } catch (IOException ignored) {
                // Shouldn't happen
            }

            return msg;
        } catch (JMSException e) {
            return null;
        }
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        try {
            final Session s = getSession();
            return new ActiveMQProducer(s.createProducer(new ActiveMQQueue(name)), s);
        } catch (JMSException | ConnectorException e) {
            log.error("getQueueProducer, throwing error", e);
            return null;
        }
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
         try {
            final Session s = getSession();
            return new ActiveMQProducer(s.createProducer(new ActiveMQTopic(name)), s);
        } catch (JMSException | ConnectorException e) {
            log.error("getTopicProducer, throwing error", e);
            return null;
        }
    }
}
