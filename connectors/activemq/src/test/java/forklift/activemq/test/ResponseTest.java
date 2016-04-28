package forklift.activemq.test;

import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.decorators.Queue;
import forklift.producers.ForkliftResultResolver;
import forklift.producers.ForkliftSyncProducer;
import forklift.producers.ForkliftSyncProducerI;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class ResponseTest {
    @Before
    public void before() {
        TestServiceManager.start();
    }

    @After
    public void after() {
        TestServiceManager.stop();
    }

//    @Test
    public void testStringResp() throws Exception {
        final ForkliftConnectorI connector = TestServiceManager.getConnector();

        // Create a resolver to resolve future responses
        final ForkliftResultResolver<String> resolver = new ForkliftResultResolver<String>();

        // Collect a future for each result expected so they can be verified.
        final List<Future<String>> futures = new ArrayList<>();

        // Produce messages with a sync producer that connects the producer, resolver, and response uri together.
        try (ForkliftSyncProducerI<String> producer = new ForkliftSyncProducer<String>(
                connector.getQueueProducer("response"), resolver, "queue://" + ResponseConsumerString.class.getAnnotation(Queue.class).value())) {
            for (int i = 0; i < 100; i++)
                futures.add(producer.send("a"));
        }

        // Consumer messages until they are all gone.
        ResponseConsumerString.resolver = resolver;
        final Consumer c = new Consumer(ResponseConsumerString.class, connector);
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            Assert.assertTrue(f.isDone());
            try {
                Assert.assertEquals("test", f.get());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    @Test
    public void testMapResp() throws Exception {
        final ForkliftConnectorI connector = TestServiceManager.getConnector();

        // Create a resolver to resolve future responses
        final ForkliftResultResolver<Map<String, String>> resolver = new ForkliftResultResolver<>();

        // Collect a future for each result expected so they can be verified.
        final List<Future<Map<String,String>>> futures = new ArrayList<>();

        // Produce messages with a sync producer that connects the producer, resolver, and response uri together.
        try (ForkliftSyncProducerI<Map<String, String>> producer = new ForkliftSyncProducer<>(
                connector.getQueueProducer("response"), resolver, "queue://" + ResponseConsumerMap.class.getAnnotation(Queue.class).value())) {
            for (int i = 0; i < 100; i++)
                futures.add(producer.send("{}"));
        }

        // Consumer messages until they are all gone.
        ResponseConsumerMap.resolver = resolver;
        final Consumer c = new Consumer(ResponseConsumerMap.class, connector);
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            Assert.assertTrue(f.isDone());
            try {
                Map<String, String> map = f.get();
                Assert.assertTrue(map.containsKey("x"));
                Assert.assertEquals("x", map.get("x"));
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    @Test
    public void testObjResp() throws Exception {
        final ForkliftConnectorI connector = TestServiceManager.getConnector();

        // Create a resolver to resolve future responses
        final ForkliftResultResolver<ResponseObj> resolver = new ForkliftResultResolver<>();

        // Collect a future for each result expected so they can be verified.
        final List<Future<ResponseObj>> futures = new ArrayList<>();

        // Produce messages with a sync producer that connects the producer, resolver, and response uri together.
        try (ForkliftSyncProducerI<ResponseObj> producer = new ForkliftSyncProducer<>(
                connector.getQueueProducer("response"), resolver, "queue://" + ResponseConsumerMap.class.getAnnotation(Queue.class).value())) {
            for (int i = 0; i < 100; i++)
                futures.add(producer.send("{}"));
        }

        // Consumer messages until they are all gone.
        ResponseConsumerObj.resolver = resolver;
        final Consumer c = new Consumer(ResponseConsumerObj.class, connector);
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            Assert.assertTrue(f.isDone());
            try {
                ResponseObj o = f.get();
                Assert.assertEquals("Dude", o.getName());
                Assert.assertEquals(new Integer(22), o.getAge());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }
}
