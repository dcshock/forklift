package forklift.datadog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DatadogCollectorTest {

    @Test
    public void testQueueName() {
        assertEquals("queue", DatadogCollector.getQueueName("queue:1"));
        assertEquals("queue:1", DatadogCollector.getQueueName("queue:1:1"));
        assertEquals("queue", DatadogCollector.getQueueName("queue"));
        assertEquals("", DatadogCollector.getQueueName(""));
        assertEquals("", DatadogCollector.getQueueName(":queuee1"));
        assertEquals("", DatadogCollector.getQueueName(null));
    }

    @Test
    public void testIncrement() {
        DatadogCollector ddc = new DatadogCollector();
        assertEquals(0.0, ddc.increment("", ""));
        assertEquals(0.0, ddc.increment(null, "abc"));
        assertEquals(0.0, ddc.increment("abc", null));
        assertEquals(0.0, ddc.increment(null, null));

        assertEquals(1.0, ddc.increment("queue", "pending"));
        assertEquals(2.0, ddc.increment("queue", "pending"));
        assertEquals(1.0, ddc.increment("queue", "complete"));
        assertEquals(3.0, ddc.increment("queue", "pending"));

        assertEquals(1.0, ddc.increment("topic", "pending"));
        assertEquals(2.0, ddc.increment("topic", "pending"));
        assertEquals(1.0, ddc.increment("topic", "complete"));
        assertEquals(3.0, ddc.increment("topic", "pending"));
    }
}