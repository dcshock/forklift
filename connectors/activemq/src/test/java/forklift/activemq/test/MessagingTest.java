package forklift.activemq.test;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.decorators.OnMessage;
import forklift.decorators.OnValidate;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.decorators.Queue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;

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

    @BeforeAll
    public static void before() {
        TestServiceManager.start();
    }

    @AfterAll
    public static void after() {
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
        if (!m.getId().equals("" + i)) {
            ordered = false;
            System.out.println(m.getId() + ":" + i);
        }
        System.out.println(m.getId());
    }

    @Test
    public void test() throws JMSException, ConnectorException, ProducerException {
        int msgCount = 100;
        TestServiceManager.getForklift().getLifeCycle().register(this.getClass());
        ForkliftProducerI producer = TestServiceManager.getForklift().getConnector().getQueueProducer("q1");
        for (int i = 0; i < msgCount; i++) {
            final ForkliftMessage m = new ForkliftMessage();
            m.setId("" + i);
            m.setMsg("x=Hello, message test");
            producer.send(m);
        }

        final Consumer c = new Consumer(getClass(), TestServiceManager.getForklift());
        // Shutdown the consumer after all the messages have been processed.
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
            assertTrue(ordered);
            assertTrue(called.get() == msgCount, "called was not == " + msgCount + "  --  " + called.get());
        });

        // Start the consumer.
        c.listen();

        assertTrue(called.get() > 0);
    }
}
