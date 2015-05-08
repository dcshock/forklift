package forklift.producers;

import javax.jms.JMSException;

public class ProducerException extends JMSException {
	public ProducerException(String s) {
        super(s);
    }
}