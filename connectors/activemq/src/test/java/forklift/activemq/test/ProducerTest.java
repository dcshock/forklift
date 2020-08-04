package forklift.activemq.test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.OnMessage;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.decorators.Queue;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    public void before() {
        TestServiceManager.start();
        called.set(0);
        ordered = true;
        isInjectNull = true;
        isHeadersSet = false;
        isPropsSet = false;
        isPropOverwritten = true;
    }

    @AfterAll
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
            if (!m.getId().equals("" + i)) {
                ordered = false;
                System.out.println(m.getId() + " -:- " + i);
            }
            if(ordered) {
                System.out.println(m.getProperties().get("Eye")+ " -:- " + i + " || " + m.getProperties().get("Eye").equals("ball"));
                System.out.println(m.getProperties().get("Foo")+ " -:- FOO");
                // System.out.println(m.getJmsMsg().getJMSType()+ " -:- Type");
                System.out.println("JMSCorrelationsID -:- "+ m.getId());
            }
            isPropOverwritten = m.getProperties().get("Eye").equals("ball") ? false : true;
            isPropsSet = m.getProperties().get("Foo").equals("Bar") ? true : false;
            isHeadersSet = m.getHeaders().get(Header.Type).equals("presetHeaderAction") ? true : false;
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

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(called.get() == msgCount, "called was not == " + msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Test
    public void testSendObjectMessage() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final TestMessage m = new TestMessage(new String("x=producer object send test"), i);
            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(called.get() == msgCount, "called was not == " + msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
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

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(called.get() == msgCount, "called was not == " + msgCount);
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }

    @Test
    public void testSendTripleThreat() throws ConnectorException, ProducerException {
        int msgCount = 10;
        ForkliftProducerI producer = TestServiceManager.getConnector().getQueueProducer("q2");
        for (int i = 0; i < msgCount; i++) {
            final ForkliftMessage m = new ForkliftMessage();
            m.setId("" + i);
            // final ActiveMQTextMessage m = new ActiveMQTextMessage();
            // m.setJMSCorrelationID("" + i);
            m.setMsg("x=producer overload test");
            Map<Header, Object> headers = new HashMap<>();
            headers.put(Header.Type, "SeriousBusiness");
            Map<String, String> props = new HashMap<>();
            props.put("Foo", "bar");
            props.put("Eye", "" + i);
            producer.send(headers, props, m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(ordered);
            assertTrue(called.get() == msgCount, "called was not == " + msgCount);
            assertTrue(isInjectNull == false, "injectedProducer is null");
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
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

        Map<String, String> props = new HashMap<>();
        props.put("Foo", "Bar");
        producer.setProperties(props);

        for (int i = 0; i < msgCount; i++) {
            final ForkliftMessage m = new ForkliftMessage();
            try {
                m.setId("" + i);
                m.setMsg("x=producer preset test");
                m.getProperties().put("Eye", "ball");
            } catch (Exception ignored) {
            }
            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(ordered);
            assertTrue(called.get() == msgCount, "called was not == " + msgCount);
            assertTrue(!isPropOverwritten, "Message properties were overwritten");
            assertTrue(isPropsSet, "Message properties were not set");
            assertTrue(isHeadersSet, "Message headers were not set");
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
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
