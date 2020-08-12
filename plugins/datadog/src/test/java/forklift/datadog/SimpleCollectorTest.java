package forklift.datadog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SimpleCollectorTest {
    @Test
    public void testQueueName() {
        assertEquals("queue", DatadogCollector.getConsumerName("queue:1"));
        assertEquals("queue:1", DatadogCollector.getConsumerName("queue:1:1"));
        assertEquals("queue", DatadogCollector.getConsumerName("queue"));
        assertEquals("", DatadogCollector.getConsumerName(""));
        assertEquals("", DatadogCollector.getConsumerName(":queuee1"));
        assertEquals("", DatadogCollector.getConsumerName(null));
    }

    @Test
    public void testIncrement() {
        SimpleCollector ddc = new SimpleCollector();
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
