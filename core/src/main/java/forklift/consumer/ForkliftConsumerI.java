package forklift.consumer;

import javax.jms.JMSException;
import javax.jms.Message;

public interface ForkliftConsumerI {
	Message receive(long timeout) throws JMSException;
	void close() throws JMSException;
}