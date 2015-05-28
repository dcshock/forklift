package forklift.activemq.test;

import forklift.Forklift;
import forklift.connectors.ActiveMQConnector;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;

/**
 * Contains all of the necessary connection management code that tests
 * need in order to run against the activemq broker and forklift. This
 * manager assumes that only a single broker is needed for testing.
 * @author mconroy
 *
 */
public class TestServiceManager {
    private static final Object lock = new Object();
    private static final String brokerUrl = "failover:(tcp://127.0.0.1:61618)";
    private static Integer count = 0;
    private static BrokerService activemq;
    private static ActiveMQConnector connector;
    private static Forklift forklift;

    public static void start() {
        synchronized (lock) {
            if (forklift != null && forklift.isRunning())
                return;

            try {
                activemq = new BrokerService();
                activemq.addConnector("tcp://127.0.0.1:61618");
                activemq.start();

                // Verify that we can get an activemq connection to the broker.
                connector = new ActiveMQConnector(brokerUrl);
                connector.start();

                forklift = new Forklift();
                forklift.start(connector);

                count++;
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static void stop() {
        synchronized (lock) {
            // Don't shutdown the connector if there are still classes using it.
            count--;
            if (count > 0)
                return;

            // Kill the broker and cleanup the testing data.
            try {
                forklift.shutdown();
                activemq.stop();
                FileUtils.deleteDirectory(activemq.getDataDirectoryFile());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    public static ActiveMQConnector getConnector() {
        synchronized (lock) {
            return connector;
        }
    }

    public static Forklift getForklift() {
        synchronized (lock) {
            return forklift;
        }
    }
}
