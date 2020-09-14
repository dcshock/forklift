package forklift.datadog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    // System properties in the following tests get set at system level
    // Make sure each test is using unique props
    @Test
    public void testPropsShutoff() {
        SimpleCollector ddc = new SimpleCollector();
        // No props set -- increments
        assertFalse(ddc.isTurnedOff("queue", "pending", "testpropoff"));
        // Turn off all pending lifecycle increments and test
        System.setProperty("testpropoff.pending", "false");
        assertTrue(ddc.isTurnedOff("queue", "pending", "testpropoff"));
        // Make sure others are still on
        assertFalse(ddc.isTurnedOff("queue", "validate", "testpropoff"));
        // Using anything other than true should shut it down
        System.setProperty("testpropoff.validate", "true");
        assertFalse(ddc.isTurnedOff("queue", "validate", "testpropoff"));
        System.setProperty("testpropoff.validate", "hellyeah");
        assertTrue(ddc.isTurnedOff("queue", "validate", "testpropoff"));
    }

    @Test
    public void testPropsQueueOn() {
        SimpleCollector ddc = new SimpleCollector();
        // No props set -- increments
        assertEquals(1.0, ddc.increment("queue", "pending", "testpropqon"));
        // Turn off all pending lifecycle increments and test
        System.setProperty("testpropqon.pending", "false");
        assertEquals(0.0, ddc.increment("queue", "pending", "testpropqon"));
        // Turn on specifc queue pending and test
        System.setProperty("testpropqon.pending.queue", "true");
        assertEquals(2.0, ddc.increment("queue", "pending", "testpropqon"));
        // Make sure other pendings still off
        assertEquals(0.0, ddc.increment("queue2", "pending", "testpropqon"));
        //Make sure non-pending still on
        assertEquals(1.0, ddc.increment("queue", "validate", "testpropqon"));
        // Turn on other increment shouldn't change
        System.setProperty("testpropqon.validate.queue", "true");
        assertEquals(2.0, ddc.increment("queue", "validate", "testpropqon"));
        // Using anything but "true" should still be false
        System.setProperty("testpropqon.validate.queue", "on");
        assertEquals(0.0, ddc.increment("queue", "validate", "testpropqon"));
        System.setProperty("testpropqon.validate.queue", "yes");
        assertEquals(0.0, ddc.increment("queue", "validate", "testpropqon"));
    }

    @Test
    public void testPropsQueueOff() {
        SimpleCollector ddc = new SimpleCollector();
        // No props set -- increments
        assertEquals(1.0, ddc.increment("queue", "pending", "testpropqoff"));
        System.setProperty("testpropqoff.pending.queue", "true");
        assertEquals(2.0, ddc.increment("queue", "pending", "testpropqoff"));
        System.setProperty("testpropqoff.pending.queue", "false");
        assertEquals(0.0, ddc.increment("queue", "pending", "testpropqoff"));
        assertEquals(1.0, ddc.increment("queue2", "pending", "testpropqoff"));
    }

    @Test
    public void testTimerStart() {
        SimpleCollector ddc = new SimpleCollector();
        assertNull(ddc.timerStart(null, null));
        assertNull(ddc.timerStart("", ""));
        assertNull(ddc.timerStart(null, "abc"));
        assertNull(ddc.timerStart("", "pending"));
        assertNull(ddc.timerStart("pending", ""));
        assertNull(ddc.timerStart("pending", null));
        final var pq = ddc.timerStart("queue", "pending");
        var vq = ddc.timerStart("queue", "validate");
        assertNotNull(pq);
        assertNotSame(pq, vq);
        // Make sure a second call on same thread doesn't create a new one
        var pq2 = ddc.timerStart("queue", "pending");
        assertSame(pq, pq2);


        // Now create another on a different thread and make sure they
        // aren't the same. Each thread should get it's own queue.lifecycle timer.
        Thread thread = new Thread() {
            public void run() {
                var ntq = ddc.timerStart("queue", "pending");
                assertNotNull(pq);
                assertNotNull(ntq);
                assertNotSame(pq, ntq);
            }
        };

        thread.start();
    }

    @Test
    public void testTimerStop() {
        SimpleCollector ddc = new SimpleCollector();
        // Trying to stop on bad args should return 0.0
        assertEquals(0.0, ddc.timerStop(null, null));
        assertEquals(0.0, ddc.timerStop("", ""));
        assertEquals(0.0, ddc.timerStop(null, "abc"));
        assertEquals(0.0, ddc.timerStop("", "pending"));
        assertEquals(0.0, ddc.timerStop("pending", ""));
        assertEquals(0.0, ddc.timerStop("pending", null));
        // Timer wasn't started, doesn't exist
        assertEquals(0.0, ddc.timerStop("queue", "pending"));
        ddc.timerStart("queue", "pending");
        assertEquals(1.0, ddc.timerStop("queue", "pending"));
        // Make sure after it was stopped that it cleared itself
        assertEquals(0.0, ddc.timerStop("queue", "pending"));
        // Make sure that 1 stop doesn't interrupt second timer
        ddc.timerStart("queue", "pending");
        ddc.timerStart("queue", "validate");
        assertEquals(1.0, ddc.timerStop("queue", "validate"));
        // The count increases, second call with this registry
        assertEquals(2.0, ddc.timerStop("queue", "pending"));
    }
}
