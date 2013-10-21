package forklift.connectors;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class ActiveMQConnector implements ForkliftConnectorI {
    private ActiveMQConnectionFactory factory;
    private ActiveMQConnection conn; 
    private String brokerUrl;
    
    public ActiveMQConnector() {
        
    }
    
    public ActiveMQConnector(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }
    
    @Override
    public synchronized void start() 
      throws ConnectorException {
        if (brokerUrl == null)
            throw new ConnectorException("brokerUrl wasn't set");
        
        factory = new ActiveMQConnectionFactory("", "", brokerUrl);
    }

    @Override
    public synchronized void stop() 
      throws ConnectorException {
        if (conn != null)
            try {
                conn.close();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }
    }

    @Override
    public synchronized Connection getConnection() 
      throws ConnectorException {
        if (conn == null || !conn.isStarted())
            try {
                conn = (ActiveMQConnection)factory.createConnection();
                conn.start();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }
        
        if (conn == null)
            throw new ConnectorException("Could not create connection to activemq.");
        
        return conn;
    }
}
