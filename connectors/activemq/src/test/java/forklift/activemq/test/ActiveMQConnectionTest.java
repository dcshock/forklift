package forklift.activemq.test;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import forklift.connectors.ActiveMQConnector;

public class ActiveMQConnectionTest {
    private static final String brokerUrl = "tcp://127.0.0.1:61617";
    
    @Test
    public void connect() throws Exception {
        // Spin-up the broker for testing.
        final BrokerService activemq = new BrokerService();        
        activemq.addConnector(brokerUrl);
        activemq.start();
        
        try {
            // Verify that we can get an activemq connection to the broker.
            final ActiveMQConnector connector = new ActiveMQConnector(brokerUrl);
            connector.start();
            Assert.assertNotNull(connector.getConnection());
            Assert.assertTrue(connector.getConnection() instanceof ActiveMQConnection);
            
            // Verify that the connector got us a good connection.
            ActiveMQConnection connection = (ActiveMQConnection)connector.getConnection();
            Assert.assertTrue(connection.isStarted());
            
            // Stop the connector and verify that the connection stopped.
            connector.stop();
            Assert.assertFalse(connection.isStarted());
            
            // verify that a call to the connector for a new connection reconnects.
            connection = (ActiveMQConnection)connector.getConnection();
            Assert.assertTrue(connection.isStarted());
            connector.stop();
            Assert.assertFalse(connection.isStarted());
        } catch (Throwable e) {
            Assert.fail();
        } finally {
            // Kill the broker and cleanup the testing data.
            activemq.stop();
            FileUtils.deleteDirectory(activemq.getDataDirectoryFile());
        }
    }
}
