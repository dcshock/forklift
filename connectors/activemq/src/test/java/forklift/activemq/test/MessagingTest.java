package forklift.activemq.test;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;

@Queue("q1")
public class MessagingTest {
	private static AtomicInteger called = new AtomicInteger(0);
	private static boolean ordered = true;
	
	@forklift.decorators.Message
	private ForkliftMessage m;
	
	@Before
	public void before() {
		TestServiceManager.start();
	}

	@After
	public void after() {
		TestServiceManager.stop();
	}
	
	@OnMessage
	public void onMessage() {
		if (m == null)
			return;
		
		int i = called.getAndIncrement();
		System.out.println(Thread.currentThread().getName() + m);
		try {
			if (!m.getJmsMsg().getJMSCorrelationID().equals("" + i)) {
				ordered = false;
				System.out.println(m.getJmsMsg().getJMSCorrelationID() + ":" + i);
			}
			System.out.println(m.getJmsMsg().getJMSCorrelationID());
		} catch (JMSException e) {
		}
	}
	
    @Test
    public void test() throws JMSException, ConnectorException {
    	int msgCount = 100;
    	
    	final ForkliftConnectorI connector = TestServiceManager.getForklift().getConnector();
    	final MessageProducer producer = connector.getQueueProducer("q1");
        for (int i = 0; i < msgCount; i++) {
        	final Message m = new ActiveMQTextMessage();
        	m.setJMSCorrelationID("" + i);
        	producer.send(m);
        }
        producer.close();

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();

            Assert.assertTrue(ordered);
            Assert.assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();
        
        Assert.assertTrue(called.get() > 0);
    }
}
