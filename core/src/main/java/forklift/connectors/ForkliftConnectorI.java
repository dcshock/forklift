package forklift.connectors;

import javax.jms.Connection;

public interface ForkliftConnectorI {
    void start();
    void stop();
    Connection getConnection();
}
