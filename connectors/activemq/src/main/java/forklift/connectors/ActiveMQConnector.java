package forklift.connectors;

<<<<<<< HEAD
import forklift.producers.ActiveMQProducer;
import forklift.producers.ForkliftProducerI;
=======
import forklift.ActiveMQHeaders;
import forklift.message.Header;
>>>>>>> develop

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

<<<<<<< HEAD
import java.util.Arrays;
=======
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
>>>>>>> develop

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class ActiveMQConnector implements ForkliftConnectorI {
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
    public MessageConsumer getQueue(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return s.createConsumer(s.createQueue(name));
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public MessageConsumer getTopic(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return s.createConsumer(s.createTopic(name));
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
            return new ActiveMQProducer(getSession().createProducer(new ActiveMQQueue(name)), getSession());
        } catch (JMSException | ConnectorException e) {
            System.out.println("getQueueProducer, throwing error");
            return null;
        }
    }
    
    @Override
    public ForkliftProducerI getTopicProducer(String name) {
         try {
            return new ActiveMQProducer(getSession().createProducer(new ActiveMQTopic(name)), getSession());
        } catch (JMSException | ConnectorException e) {
            System.out.println("getTopicProducer, throwing error");
            return null;
        }
    }

    @Override
    public void register(Object instance) {
        try {
            Arrays.stream(instance.getClass().getDeclaredFields())
                                    .filter(field -> field.getType() == ForkliftProducerI.class)
                                    .forEach(field -> {
                                        forklift.decorators.Producer annotation = field.getAnnotation(forklift.decorators.Producer.class);
                                        field.setAccessible(true);
                                        try {
                                            if (annotation.queue().length() > 0) {
                                                field.set(instance, this.getQueueProducer(annotation.queue()));
                                            } else if (annotation.topic().length() > 0) {
                                                field.set(instance, this.getTopicProducer(annotation.topic()));
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
