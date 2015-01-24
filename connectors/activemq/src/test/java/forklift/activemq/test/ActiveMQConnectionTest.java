package forklift.activemq.test;

import org.apache.activemq.ActiveMQConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import forklift.connectors.ForkliftConnectorI;

public class ActiveMQConnectionTest {
    @Before
    public void before() {
    	Initializer.start();
    }
    
    @After
    public void after() {
    	Initializer.stop();
    }
    
    @Test
    public void connect() throws Exception {
        // Verify that we can get an activemq connection to the broker.
    	ForkliftConnectorI connector = Initializer.getConnector();
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
    }
}
