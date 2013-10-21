package forklift.connectors;

import javax.jms.Connection;

public interface ForkliftConnectorI {
    void start() throws ConnectorException;
    void stop() throws ConnectorException;
    Connection getConnection() throws ConnectorException;
}
