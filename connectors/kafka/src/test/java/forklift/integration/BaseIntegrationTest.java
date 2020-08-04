package forklift.integration;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import forklift.schemas.AvroMessage;
import forklift.source.decorators.Queue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract class which provides support for some common integration testing scenarios.
 * <pre>
 *     1.  Starts up and exposes a TestService Manager at the start of every test.
 *         The testServiceManager starts up embedded Zookeeper, Kafka, and Schema-Registry servers
 *     2.  Various Consumer classes
 *     3.  sentMessageIds and consumedMessageIds.  Implementing classes should ensure that they
 *         populate the sentMessageIds set.
 *     4.  The messageAsserts function which provides some useful output and asserts that the sentMessageIds set
 *         and consumedMessageIds set are equal.
 * </pre>
 *
 */
public abstract class BaseIntegrationTest {
    protected static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);
    protected static Set<String> sentMessageIds = ConcurrentHashMap.newKeySet();
    protected static Set<String> consumedMessageIds = ConcurrentHashMap.newKeySet();
    protected TestServiceManager serviceManager;
    protected final int maxTimeouts = 5;
    protected int timeouts = 0;

    @AfterAll
    public void after() {
        serviceManager.stop();
    }

    @BeforeAll
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        sentMessageIds = ConcurrentHashMap.newKeySet();
        consumedMessageIds = ConcurrentHashMap.newKeySet();
    }

    protected void messageAsserts(){
        log.info("SentIds: " + sentMessageIds.size() + " consumedIds: " + consumedMessageIds.size());
        assertTrue(sentMessageIds.equals(consumedMessageIds));
        assertTrue(sentMessageIds.size() > 0);
    }


    @Queue("forklift-string-topic")
    public static class StringConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }

    @MultiThreaded(10)
    @Queue("forklift-string-topic")
    public static class MultiThreadedStringConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }

    @Queue("forklift-object-topic")
    public static class ForkliftObjectConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private TestMessage testMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (testMessage == null || testMessage.getText() == null) {
                return;
            }
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }

    @Queue("forklift-map-topic")
    public static class ForkliftMapConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private Map<String, String> mapMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (mapMessage == null || mapMessage.size() == 0) {
                return;
            }
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }

    @Queue("forklift-avro-topic")
    public static class ForkliftAvroConsumer {

        @forklift.decorators.Message
        private ForkliftMessage forkliftMessage;

        @forklift.decorators.Message
        private AvroMessage value;

        @Producer(queue = "forklift-avro-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            consumedMessageIds.add(forkliftMessage.getId());
        }
    }
}
