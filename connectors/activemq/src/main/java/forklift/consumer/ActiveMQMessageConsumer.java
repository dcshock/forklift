package forklift.consumer;

import forklift.consumer.ForkliftConsumerI;

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

	public Message receive(long timeout) throws JMSException {
		return consumer.receive(timeout);
	}

	public void close() throws JMSException {
		if (consumer != null)
			consumer.close();

		if (s != null)
			s.close();
	}
}