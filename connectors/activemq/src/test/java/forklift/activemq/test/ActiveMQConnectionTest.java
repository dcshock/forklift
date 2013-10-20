package forklift.activemq.test;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class ActiveMQConnectionTest {
    @Test
    public void spinup() throws Exception {
        BrokerService activemq = new BrokerService();
        activemq.addConnector("tcp://127.0.0.1:61617");
        activemq.start();
        activemq.stop();
        FileUtils.deleteDirectory(activemq.getDataDirectoryFile());
    }
}
