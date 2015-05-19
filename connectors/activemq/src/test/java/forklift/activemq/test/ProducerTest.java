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
import forklift.decorators.Topic;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

@Queue("q2")
public class ProducerTest {
    private static AtomicInteger called = new AtomicInteger(0);
    private static boolean ordered = true;

    @forklift.decorators.Message
    private ForkliftMessage m;

    @forklift.decorators.Producer(queue="q2")
    private ForkliftProducerI producer;

    @Before
    public void before() {
        producer = null;
        TestServiceManager.start();
        TestServiceManager.getConnector().register(this);
        called.set(0);
        ordered = true;
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
                System.out.println(m.getJmsMsg().getJMSCorrelationID() + " -:- " + i);
            }
            if(ordered) {
                System.out.println(m.getJmsMsg().getStringProperty("Eye")+ " -:- " + i);
                System.out.println(m.getJmsMsg().getStringProperty("Foo")+ " -:- FOO");
                System.out.println(m.getJmsMsg().getJMSType()+ " -:- Type");
                System.out.println("JMSCorrelationsID -:- "+ m.getJmsMsg().getJMSCorrelationID());
            }
        } catch (JMSException e) {
        }
    }

    @Test
    public void testStringMessage() throws ProducerException, ConnectorException {
        int msgCount = 100;

        for (int i = 0; i < msgCount; i++) {
            String msg = new String("sending all the text, producer test");
            producer.send(msg);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            Assert.assertTrue("called was not == " + msgCount, called.get() == msgCount);
        });

        // Start the consumer.
        c.listen();

        Assert.assertTrue(called.get() > 0);
    }

    @Test
    public void testProducerSendOverload() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 100;

        for (int i = 0; i < msgCount; i++) {
            final ActiveMQTextMessage m = new ActiveMQTextMessage();
            m.setJMSCorrelationID("" + i);
            m.setText("x=producer overload test");
            Map<Header, String> headers = new HashMap<>();
            headers.put(Header.Type, "SeriousBusiness");
            Map<String, Object> props = new HashMap<>();
            props.put("Foo", "bar");
            props.put("Eye", "" + i);     
            producer.send(headers, props, new ForkliftMessage(m));
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
