package forklift.activemq.test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;
import forklift.source.ActionSource;
import forklift.source.LogicalSource;
import forklift.source.SourceI;

import javax.inject.Named;
import javax.jms.Connection;

/**
 * Wrap the activemq connector with a spring annotation so that forklift can
 * resolve the provider.
 * @author mconroy
 *
 */
@Named
public class ActiveMQForkliftConnector implements ForkliftConnectorI {

    @Override
    public void start() throws ConnectorException {
        TestServiceManager.getConnector().start();
    }

    @Override
    public void stop() throws ConnectorException {
        TestServiceManager.getConnector().stop();
    }

    public Connection getConnection() throws ConnectorException {
        return TestServiceManager.getConnector().getConnection();
    }

    @Override
    public ForkliftConsumerI getConsumerForSource(SourceI source) throws ConnectorException {
        return TestServiceManager.getConnector().getConsumerForSource(source);
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        return TestServiceManager.getConnector().getQueueProducer(name);
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
        return TestServiceManager.getConnector().getTopicProducer(name);
    }

    @Override
    public ActionSource mapSource(LogicalSource source) {
        return TestServiceManager.getConnector().mapSource(source);
    }
}
