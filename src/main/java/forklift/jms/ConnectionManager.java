package forklift.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.springframework.jms.connection.SingleConnectionFactory;


public class ConnectionManager implements ConnectionManagerI {
    public ConnectionManager() {
    }

    @Override
    public void start() {
        ConnectionFactory factory = new SingleConnectionFactory();
    }

    @Override
    public void stop() {

    }

    @Override
    public Connection getConnection() {
        return null;
    }
}
