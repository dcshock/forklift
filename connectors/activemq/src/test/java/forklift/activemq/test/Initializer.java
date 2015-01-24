package forklift.activemq.test;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import forklift.Forklift;
import forklift.connectors.ActiveMQConnector;

@Configuration
@ComponentScan(value={"forklift"}, excludeFilters={})
public class Initializer {
	private static final Object lock = new Object();
	private static final String brokerUrl = "tcp://127.0.0.1:61617";
	private static Integer count = 0;
	private static BrokerService activemq;
	private static ActiveMQConnector connector;
	private static Forklift forklift;
	
	public static void start() {
		synchronized (lock) {
			if (connector != null)
				return;

			try {
				activemq = new BrokerService();
				activemq.addConnector(brokerUrl);
				activemq.start();
				
				forklift = new Forklift();
				forklift.start();
				
				// Verify that we can get an activemq connection to the broker.
	            connector = new ActiveMQConnector(brokerUrl);
	            connector.start();
	            
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
				connector.stop();
				connector = null;
				
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
