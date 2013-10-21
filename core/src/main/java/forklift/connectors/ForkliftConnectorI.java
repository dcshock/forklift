package forklift.connectors;

import javax.jms.Connection;
import javax.jms.MessageConsumer;

public interface ForkliftConnectorI {
    void start() throws ConnectorException;
    void stop() throws ConnectorException;
    Connection getConnection() throws ConnectorException;
//    Session getSession() throws ConnectorException;
    MessageConsumer getQueue(String name) throws ConnectorException;
    MessageConsumer getTopic(String name) throws ConnectorException;
}
