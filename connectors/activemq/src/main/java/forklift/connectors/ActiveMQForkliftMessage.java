package forklift.connectors;

import javax.jms.JMSException;

import javax.jms.Message;

import forklift.connectors.ForkliftMessage;

public class ActiveMQForkliftMessage extends ForkliftMessage {
    private Message jmsMsg;

    public ActiveMQForkliftMessage(Message jmsMsg) {
        this.jmsMsg = jmsMsg;
    }

    public boolean acknowledge() {
        try {
            jmsMsg.acknowledge();
            return true;
        } catch (JMSException e) {
            return false;
        }
    }

    public Message getJmsMsg() {
        return this.jmsMsg;
    }
}
