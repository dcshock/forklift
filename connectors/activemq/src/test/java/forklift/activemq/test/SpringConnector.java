package forklift.activemq.test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;

import org.springframework.stereotype.Component;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;

/**
 * Wrap the active mq connector with a spring annotation.
 * @author mconroy
 *
 */
@Component
public class SpringConnector implements ForkliftConnectorI {
	@Override
	public void start() throws ConnectorException {
		Initializer.getConnector().start();
	}

	@Override
	public void stop() throws ConnectorException {
		Initializer.getConnector().stop();
	}

	@Override
	public Connection getConnection() throws ConnectorException {
		return Initializer.getConnector().getConnection();
	}

	@Override
	public MessageConsumer getQueue(String name) throws ConnectorException {
		return Initializer.getConnector().getQueue(name);
	}

	@Override
	public MessageConsumer getTopic(String name) throws ConnectorException {
		return Initializer.getConnector().getTopic(name);
	}

	@Override
	public ForkliftMessage jmsToForklift(Message m) {
		return Initializer.getConnector().jmsToForklift(m);
	}

	@Override
	public MessageProducer getProducer(String name) {
		return Initializer.getConnector().getProducer(name);
	}
}
