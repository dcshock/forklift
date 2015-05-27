package forklift.activemq.test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.LifeCycleMonitors;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.MessageProducer;

@Queue("q1")
public class MessagingTest {
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean ordered = true;

    @forklift.decorators.Message
    private ForkliftMessage m;

    // This is null right now and is just being used to ensure the code at least tries to hit the injection code for props. 
    @forklift.decorators.Config("none")
    private Properties props;

    @forklift.decorators.Message
    private String strMsg;

    @forklift.decorators.Message
    private Map<String, String> keyvalMsg;

    @Before
    public void before() {
        TestServiceManager.start();
    }

    @After
    public void after() {
        TestServiceManager.stop();
    }

    @LifeCycle(ProcessStep.Pending)
    @LifeCycle(ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        System.out.println("GOT IT WHOA !!!!......................................... " + mr);

    };

    /*
        Ensure that validate methods are called.
     */
    @OnValidate
    public boolean onValidate() {
        return true;
    }
    @OnValidate
    public List<String> onValidateList() {
        return Collections.emptyList();
    }

    @OnMessage
    public void onMessage() {
        if (m == null || strMsg == null || keyvalMsg == null || keyvalMsg.size() != 1)
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
    public void test() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 100;
        LifeCycleMonitors.register(this.getClass());
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q1");
        for (int i = 0; i < msgCount; i++) {
            final ActiveMQTextMessage m = new ActiveMQTextMessage();
            m.setJMSCorrelationID("" + i);
            m.setText("x=Hello, message test");
            producer.send(new ForkliftMessage(m));
        }

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
