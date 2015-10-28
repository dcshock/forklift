package forklift.connectors;

import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;

import javax.jms.Connection;
import javax.jms.Message;

public interface ForkliftConnectorI {
    void start() throws ConnectorException;
    void stop() throws ConnectorException;
    Connection getConnection() throws ConnectorException;
//    Session getSession() throws ConnectorException;
    ForkliftConsumerI getQueue(String name) throws ConnectorException;
    ForkliftConsumerI getTopic(String name) throws ConnectorException;
    ForkliftProducerI getQueueProducer(String name);
    ForkliftProducerI getTopicProducer(String name);

    /**
     * Convert a jms message to a forklift message.
     * @param m - the message to process
     * @return - a new ForkliftMessage.
     */
    ForkliftMessage jmsToForklift(Message m);
}
