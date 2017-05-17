package forklift.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import forklift.message.ActiveMQHeaders;
import forklift.connectors.ActiveMQForkliftMessage;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ForkliftConsumerI;
import forklift.message.Header;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;

import java.util.Optional;
import java.util.UUID;
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
            return null;

        try {
            final ActiveMQForkliftMessage msg = new ActiveMQForkliftMessage(m);

            Optional<String> messageId = Optional.ofNullable(m.getJMSCorrelationID())
                .filter(id -> !id.isEmpty());

            // use JMS message id if no correlation id is given
            if (!messageId.isPresent()) {
                messageId = Optional.ofNullable(m.getJMSMessageID())
                    .filter(id -> !id.isEmpty());
            }

            msg.setId(messageId.orElseGet(() -> UUID.randomUUID().toString().replaceAll("-", "")));

            if (m instanceof ActiveMQTextMessage) {
                msg.setMsg(((ActiveMQTextMessage)m).getText());
            } else {
                msg.setFlagged(true);
                msg.setWarning("Unexpected message type: " + m.getClass().getName());
            }

            Map<Header, Object> headers = new HashMap<>();
            ActiveMQMessage amq = (ActiveMQMessage)msg.getJmsMsg();
            // Build headers
            for (Header h : Header.values()) {
                headers.put(h, ActiveMQHeaders.getFunctions().get(h).get(amq));
            }
            msg.setHeaders(headers);

            // Build properties
            final Map<String, String> props = new HashMap<>();
            try {
                amq.getProperties().forEach((k, v) -> {
                    props.put(k, v.toString());
                });
                msg.setProperties(props);
            } catch (IOException ignored) {}

            return msg;
        } catch (JMSException e) {
            return null;
        }
    }
}
