package forklift.connectors;

import forklift.producers.ActiveMQProducer;
import forklift.producers.ActiveMQProducerInfo;
import forklift.producers.ForkliftProducerI;import forklift.producers.ProducerInfoI;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;

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
            return msg;
        } catch (JMSException e) {
            return null;
        }
    }

	@Override
	public ForkliftProducerI getQueueProducer(String name) {
		 try {
			//return toForkliftProducer(getSession().createProducer(new ActiveMQQueue(name)));
            return new ActiveMQProducer(getSession().createProducer(new ActiveMQQueue(name)));
		} catch (JMSException | ConnectorException e) {
			return null;
		}
	}
	
	@Override
	public ForkliftProducerI getTopicProducer(String name) {
		 try {
			//return toForkliftProducer(getSession().createProducer(new ActiveMQTopic(name)));
            return new ActiveMQProducer(getSession().createProducer(new ActiveMQTopic(name)));
		} catch (JMSException | ConnectorException e) {
			return null;
		}
	}

    /**
    * Convert ActiveMQMessageProducer to ForkliftProducer
    * @param - the ActiveMQMessageProducer to be converted to a ForkliftProducer
    * @return - new ForkliftProducer with all the things
    */
    private ForkliftProducerI toForkliftProducer(MessageProducer producer) {
        ActiveMQProducerInfo info = new ActiveMQProducerInfo();
        try {
            info.setDestination(producer.getDestination());
        } catch (JMSException e) {
            //throw new JMSException("Error converting MessengerProducer to ForkliftProducer");
            return null;
        }
        return new ActiveMQProducer(info);
    }
}
