package forklift.integration;

import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.AvroMessage;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by afrieze on 3/28/17.
 */
public abstract class BaseIntegrationTest {
    protected static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    protected static boolean isInjectNull = true;
    protected static Set<String> sentMessageIds = ConcurrentHashMap.newKeySet();
    protected static Set<String> consumedMessageIds = ConcurrentHashMap.newKeySet();
    protected TestServiceManager serviceManager;

    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        sentMessageIds = ConcurrentHashMap.newKeySet();
        consumedMessageIds = ConcurrentHashMap.newKeySet();
    }

    protected void messageAsserts(){
        log.info("SentIds: " + sentMessageIds.size() + " consumedIds: " + consumedMessageIds.size());
        assertTrue(sentMessageIds.containsAll(consumedMessageIds) && consumedMessageIds.containsAll(sentMessageIds));
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
            isInjectNull = injectedProducer != null ? false : true;
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
            isInjectNull = injectedProducer != null ? false : true;
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
            isInjectNull = injectedProducer != null ? false : true;
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
            isInjectNull = injectedProducer != null ? false : true;
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
            isInjectNull = injectedProducer != null ? false : true;
        }
    }
}
