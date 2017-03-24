package forklift.integration;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import com.github.dcshock.avro.schemas.AvroMessage;
import forklift.Forklift;
import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.decorators.MultiThreaded;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.integration.server.TestServiceManager;
import forklift.producers.ForkliftProducerI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RebalanceTests {

    private static final Logger log = LoggerFactory.getLogger(RebalanceTests.class);
    private static AtomicInteger called = new AtomicInteger(0);
    private static AtomicInteger messagesSent = new AtomicInteger(0);
    private static boolean isInjectNull = true;
    TestServiceManager serviceManager;


    @After
    public void after() {
        serviceManager.stop();
    }

    @Before
    public void setup() {
        serviceManager = new TestServiceManager();
        serviceManager.start();
        called.set(0);
        messagesSent.set(0);
        isInjectNull = true;
    }




    private class ForkliftServer{

        private ExecutorService executor;
        private Class[] consumerClasses;
        private Forklift forklift;
        private List<Consumer> consumers = new ArrayList<Consumer>();
        private String name;
        private volatile boolean running = false;

        public ForkliftServer(String name, ExecutorService executor, Class<?>... consumerClasses){
            this.name = name;
            this.executor = executor;
            this.consumerClasses = consumerClasses;
            try {
                this.forklift = serviceManager.newManagedForkliftInstance(name);
            } catch (StartupException e) {
               log.error("Error constructing forklift server");
            }
        }

        public ForkliftProducerI getProducer(String topicName){
            return forklift.getConnector().getTopicProducer(topicName);
        }

        public void startConsumers(){
            log.info("Starting Consumers for server: " + name);
            for (Class<?> c : consumerClasses) {
                Consumer consumer = new Consumer(c, forklift.getConnector());
                consumers.add(consumer);
                executor.submit(() -> consumer.listen());
            }
        }

        public void startProducers(){

            ForkliftProducerI producer1 = getProducer("forklift-string-topic");
            ForkliftProducerI producer2 = getProducer("forklift-map-topic");
            ForkliftProducerI producer3 = getProducer("forklift-object-topic");
            Random random = new Random();
            running = true;
            executor.execute(() -> {
                while (running) {
                    long jitter = random.nextLong() % 50;
                    try {
                        producer1.send("String message");
                        messagesSent.incrementAndGet();
                        Thread.currentThread().sleep(jitter);
                    } catch (Exception e) {
                    }
                }
            });
            executor.execute(() -> {
                while (running) {
                    long jitter = random.nextLong() % 50;
                    try {
                        final Map<String, String> m = new HashMap<>();
                        m.put("x", "producer key value send test");
                        producer2.send(m);
                        messagesSent.incrementAndGet();
                        Thread.currentThread().sleep(jitter);
                    } catch (Exception e) {
                    }
                }
            });
            executor.execute(() -> {
                while (running) {
                    long jitter = random.nextLong() % 50;
                    try {
                        final TestMessage m = new TestMessage(new String("x=producer object send test"), 1);
                        producer3.send(m);
                        messagesSent.incrementAndGet();
                        Thread.currentThread().sleep(jitter);
                    } catch (Exception e) {
                    }
                }
            });
        }

        public void stopProducers(){
            running = false;
        }

        public void shutdown(){
            stopProducers();
            log.info("Stopping Consumers for server: " + name);
            consumers.forEach(consumer -> consumer.shutdown());
            forklift.shutdown();
        }
    }
    


    @Test
    public void testRebalanceUnderLoad() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(35);
        ForkliftServer server1 = new ForkliftServer("Server1", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server2 = new ForkliftServer("Server2", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server3 = new ForkliftServer("Server3", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server4 = new ForkliftServer("Server4", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server5 = new ForkliftServer("Server5", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server6 = new ForkliftServer("Server6", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server7 = new ForkliftServer("Server7", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);

        server4.startProducers();
        server5.startProducers();
        server6.startProducers();
        server7.startProducers();
        Thread.sleep(2000);
        server1.startConsumers();
        server2.startConsumers();
        Thread.sleep(10000);
        server1.shutdown();
        Thread.sleep(5000);
        server3.startConsumers();
        Thread.sleep(5000);
        server2.shutdown();

        server4.stopProducers();
        server5.stopProducers();
        server6.stopProducers();
        server7.stopProducers();
        log.info("Messages sent: " + messagesSent.get());
        //wait to finish any processing
        for(int i = 0; i < 60 && called.get() != messagesSent.get(); i++){
            log.info("Waiting: " + i);
            Thread.sleep(1000);
        }
        server3.shutdown();
        assertEquals(messagesSent.get(), called.get());
        assertTrue(messagesSent.get() > 0);

    }

    @Test
    public void testMultipleConcurrentRebalancing() throws StartupException, InterruptedException, ConnectorException {

        ExecutorService executor = Executors.newFixedThreadPool(35);

        ForkliftServer server1 = new ForkliftServer("Server1", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server2 = new ForkliftServer("Server2", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server3 = new ForkliftServer("Server3", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server4 = new ForkliftServer("Server4", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server5 = new ForkliftServer("Server5", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server6 = new ForkliftServer("Server6", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server7 = new ForkliftServer("Server7", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server8 = new ForkliftServer("Server8", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server9 = new ForkliftServer("Server9", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);
        ForkliftServer server10 = new ForkliftServer("Server10", executor, StringConsumer.class, ForkliftMapConsumer.class, ForkliftObjectConsumer.class);

        server10.startProducers();
        Thread.sleep(500);
        server1.startConsumers();
        server2.startConsumers();
        server3.startConsumers();
        server4.startConsumers();
        server5.startConsumers();
        server6.startConsumers();
        server7.startConsumers();
        server8.startConsumers();
        server9.startConsumers();
        server10.startConsumers();
        Thread.sleep(5000);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
        Thread.sleep(5000);
        server4.shutdown();
        server5.shutdown();
        server6.shutdown();
        Thread.sleep(5000);
        server7.shutdown();
        server8.shutdown();
        server9.shutdown();


        server10.stopProducers();
        //wait to finish any processing
        for(int i = 0; i < 60 && called.get() != messagesSent.get(); i++){
            log.info("Waiting: " + i);
            Thread.sleep(1000);
        }
        log.info("Messages sent: " + messagesSent.get());
        server10.shutdown();
        assertEquals(messagesSent.get(), called.get());
        assertTrue(messagesSent.get() > 0);
    }

    private List<Consumer> setupConsumers(ForkliftConnectorI connector, Class<?>... consumersClasses) {
        List<Consumer> consumers = new ArrayList<>();
        for (Class<?> c : consumersClasses) {
            Consumer consumer = new Consumer(c, connector);
            consumers.add(consumer);
        }
        return consumers;
    }

    @Queue("forklift-string-topic")
    public static class StringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @Queue("forklift-avro-topic")
    public static class ForkliftAvroConsumer {

        @forklift.decorators.Message
        private AvroMessage value;

        @Producer(queue = "forklift-avro-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @MultiThreaded(10)
    @Queue("forklift-string-topic")
    public static class MultiThreadedStringConsumer {

        @forklift.decorators.Message
        private String value;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (value == null) {
                return;
            }
            int i = called.getAndIncrement();
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

    @Queue("forklift-map-topic")
    public static class ForkliftMapConsumer {

        @forklift.decorators.Message
        private Map<String, String> mapMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (mapMessage == null || mapMessage.size() == 0) {
                return;
            }
            int i = called.getAndIncrement();
            isInjectNull = injectedProducer != null ? false : true;
        }
    }


    @Queue("forklift-object-topic")
    public static class ForkliftObjectConsumer {

        @forklift.decorators.Message
        private TestMessage testMessage;

        @Producer(queue = "forklift-string-topic")
        private ForkliftProducerI injectedProducer;

        @OnMessage
        public void onMessage() {
            if (testMessage == null || testMessage.getText() == null) {
                return;
            }
            int i = called.getAndIncrement();
            isInjectNull = injectedProducer != null ? false : true;
        }
    }

}
