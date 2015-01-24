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
 * Wrap the activemq connector with a spring annotation so that forklift can 
 * resolve the provider.
 * @author mconroy
 *
 */
@Component
public class ActiveMQForkliftConnector implements ForkliftConnectorI {
	@Override
	public void start() throws ConnectorException {
		TestServiceManager.getConnector().start();
	}

	@Override
	public void stop() throws ConnectorException {
		TestServiceManager.getConnector().stop();
	}

	@Override
	public Connection getConnection() throws ConnectorException {
		return TestServiceManager.getConnector().getConnection();
	}

	@Override
	public MessageConsumer getQueue(String name) throws ConnectorException {
		return TestServiceManager.getConnector().getQueue(name);
	}

	@Override
	public MessageConsumer getTopic(String name) throws ConnectorException {
		return TestServiceManager.getConnector().getTopic(name);
	}

	@Override
	public ForkliftMessage jmsToForklift(Message m) {
		return TestServiceManager.getConnector().jmsToForklift(m);
	}

	@Override
	public MessageProducer getProducer(String name) {
		return TestServiceManager.getConnector().getProducer(name);
	}
}
