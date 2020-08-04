package forklift.source.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class QueueSourceTest {
    @Test
    public void testSameQueuesAreEqual() {
        final String testQueue = "test-queue";

        assertEquals(new QueueSource(testQueue),
                            new QueueSource(testQueue));
    }

    @Test
    public void testDifferentQueuesAreNotEqual() {
        assertNotEquals(new QueueSource("test-queue-1"),
                               new QueueSource("test-queue-2"));
    }

    @Test
    public void testDistinctFromTopic() {
        final String testName = "test";
        assertNotEquals(new QueueSource(testName),
                               new TopicSource(testName));
    }
}
