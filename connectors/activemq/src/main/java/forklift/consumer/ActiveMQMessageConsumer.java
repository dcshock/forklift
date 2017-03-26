package forklift.consumer;

import java.util.HashMap;
import java.util.Map;

import forklift.message.ActiveMQHeaders;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ForkliftConsumerI;
import forklift.message.Header;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

public class ActiveMQMessageConsumer implements ForkliftConsumerI {
	private MessageConsumer consumer;
	private Session s;

	public ActiveMQMessageConsumer(MessageConsumer consumer, Session s) {
		this.consumer = consumer;
		this.s = s;
	}

	public ForkliftMessage receive(long timeout) throws ConnectorException {
		try {
			return jmsToForklift(consumer.receive(timeout));
		} catch (JMSException e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	public void close() throws ConnectorException {
		try {
			if (consumer != null)
				consumer.close();

			if (s != null)
				s.close();
		} catch (JMSException e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	private ForkliftMessage jmsToForklift(Message m) {
        if (m == null) 
            return new ForkliftMessage();
            
        try {
            final ForkliftMessage msg = new ForkliftMessage();
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
            // try {
            //     msg.setProperties(amq.getProperties());
            // } catch (IOException ignored) {
            //     // Shouldn't happen
            // }

            return msg;
        } catch (JMSException e) {
            return null;
        }
    }
}