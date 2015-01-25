package forklift.connectors;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;

public interface ForkliftConnectorI {
    void start() throws ConnectorException;
    void stop() throws ConnectorException;
    Connection getConnection() throws ConnectorException;
//    Session getSession() throws ConnectorException;
    MessageConsumer getQueue(String name) throws ConnectorException;
    MessageConsumer getTopic(String name) throws ConnectorException;
    MessageProducer getQueueProducer(String name);
    MessageProducer getTopicProducer(String name);

    /**
     * Convert a jms message to a forklift message.
     * @param m - the message to process
     * @return - a new ForkliftMessage.
     */
    ForkliftMessage jmsToForklift(Message m);
}
