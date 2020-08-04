package forklift.activemq.test;

import forklift.connectors.ForkliftConnectorI;
import forklift.consumer.Consumer;
import forklift.producers.ForkliftResultResolver;
import forklift.producers.ForkliftSyncProducer;
import forklift.producers.ForkliftSyncProducerI;
import forklift.source.decorators.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ResponseTest {
    @BeforeAll
    public void before() {
        TestServiceManager.start();
    }

    @AfterAll
    public void after() {
        TestServiceManager.stop();
    }

    private static final int maxTimeouts = 5;
    private int timeouts = 0;

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
        final Consumer c = new Consumer(ResponseConsumerString.class, TestServiceManager.getForklift());
        c.setOutOfMessages((listener) -> {
            listener.shutdown();
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            assertTrue(f.isDone());
            try {
                assertEquals("test", f.get());
            } catch (Exception e) {
                fail(e.getMessage());
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
            for (int i = 0; i < 10; i++)
                futures.add(producer.send("{}"));
        }

        // Consumer messages until they are all gone.
        ResponseConsumerMap.resolver = resolver;
        final Consumer c = new Consumer(ResponseConsumerMap.class, TestServiceManager.getForklift());
        c.setOutOfMessages((listener) -> {
            timeouts++;

            if (futures.stream().allMatch(future -> future.isDone()) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            assertTrue(f.isDone());
            try {
                Map<String, String> map = f.get();
                assertTrue(map.containsKey("x"));
                assertEquals("x", map.get("x"));
            } catch (Exception e) {
                fail(e.getMessage());
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
            for (int i = 0; i < 10; i++)
                futures.add(producer.send("{}"));
        }

        // Consumer messages until they are all gone.
        ResponseConsumerObj.resolver = resolver;
        final Consumer c = new Consumer(ResponseConsumerObj.class, TestServiceManager.getForklift());
        c.setOutOfMessages((listener) -> {
            timeouts++;

            if (futures.stream().allMatch(future -> future.isDone()) || timeouts > maxTimeouts) {
                listener.shutdown();
            }
        });
        c.listen();

        // Verify that each future has been completed and has a valid data item.
        futures.forEach((f) -> {
            assertTrue(f.isDone());
            try {
                ResponseObj o = f.get();
                assertEquals("Dude", o.getName());
                assertEquals(Integer.valueOf(22), o.getAge());
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });
    }
}
