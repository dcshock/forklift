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
    private static boolean isInjectNull = true;
    private static boolean isPropsSet = false;
    private static boolean isHeadersSet = false;
    private static boolean isPropOverwritten = true;

    @forklift.decorators.Message
    private ForkliftMessage m;

    @forklift.decorators.Producer(queue="q2")
    private ForkliftProducerI injectedProducer;

    @Before
    public void before() {
        TestServiceManager.start();
        called.set(0);
        ordered = true;
        isInjectNull = true;
        isHeadersSet = false;
        isPropsSet = false;
        isPropOverwritten = true;
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
                System.out.println(m.getJmsMsg().getStringProperty("Eye")+ " -:- " + i + " || " + m.getJmsMsg().getStringProperty("Eye").equals("ball"));
                System.out.println(m.getJmsMsg().getStringProperty("Foo")+ " -:- FOO");
                System.out.println(m.getJmsMsg().getJMSType()+ " -:- Type");
                System.out.println("JMSCorrelationsID -:- "+ m.getJmsMsg().getJMSCorrelationID());
            }
            isPropOverwritten = m.getJmsMsg().getObjectProperty("Eye").equals("ball") ? false : true;
            isPropsSet = m.getJmsMsg().getStringProperty("Foo").equals("Bar") ? true : false;
            isHeadersSet = m.getJmsMsg().getJMSType().equals("presetHeaderAction") ? true : false;
        } catch (Exception e) {
        }

        isInjectNull = injectedProducer != null ? false : true;
    }

    @Test
    public void testSendStringMessage() throws ProducerException, ConnectorException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
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
    public void testSendObjectMessage() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);  
            producer.send(m);
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
    public void testSendKeyValueMessage() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final Map<String, String> m = new HashMap<>();
            m.put("x", "producer key value send test");
            producer.send(m);
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
    public void testSendTripleThreat() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final ActiveMQTextMessage m = new ActiveMQTextMessage();
            m.setJMSCorrelationID("" + i);
            m.setText("x=producer overload test");
            Map<Header, Object> headers = new HashMap<>();
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
            Assert.assertTrue("injectedProducer is null", isInjectNull == false);
        });

        // Start the consumer.
        c.listen();

        Assert.assertTrue(called.get() > 0);
    }

    @Test
    /**
    * test sending a message and see if the preset headers and props get set
    * and that they do not overwrite a message property that was defined before being sent.
    *
    **/
    public void testPresets() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        
        Map<Header, Object> headers = new HashMap<>();
        headers.put(Header.Type, "presetHeaderAction");
        producer.setHeaders(headers);
        
        Map<String, Object> props = new HashMap<>();
        props.put("Foo", "Bar");
        producer.setProperties(props);

        for (int i = 0; i < msgCount; i++) {
            final ActiveMQTextMessage m = new ActiveMQTextMessage();
            try {
                m.setJMSCorrelationID("" + i);
                m.setText("x=producer preset test");
                m.setProperty("Eye", "ball");
            } catch (Exception ignored) {
            }
            producer.send(new ForkliftMessage(m));
        }
        
        final Consumer c = new Consumer(getClass(), TestServiceManager.getConnector());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            Assert.assertTrue(ordered);
            Assert.assertTrue("called was not == " + msgCount, called.get() == msgCount);
            Assert.assertTrue("Message properties were overwritten", isPropOverwritten == false);
            Assert.assertTrue("Message properties were not set", isPropsSet == true);
            Assert.assertTrue("Message headers were not set", isHeadersSet == true);
        });

        // Start the consumer.
        c.listen();

        Assert.assertTrue(called.get() > 0);
    }

    public class TestMessage {
        private String text;
        private int someNumber;

        public TestMessage(String text, int someNumber) {
            this.text = text;
            this.someNumber = someNumber;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public int getSomeNumber() {
            return someNumber;
        }

        public void setSomeNumber(int someNumber) {
            this.someNumber = someNumber;
        }
    }
}
