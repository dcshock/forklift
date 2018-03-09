package forklift.source.sources;

import org.junit.Assert;
import org.junit.Test;

public class QueueSourceTest {
    
    public void testSameQueuesAreEqual() {
        final String testQueue = "test-queue";

        Assert.assertEquals(new QueueSource(testQueue),
                            new QueueSource(testQueue));
    }

    
    public void testDifferentQueuesAreNotEqual() {
        Assert.assertNotEquals(new QueueSource("test-queue-1"),
                               new QueueSource("test-queue-2"));
    }

    
    public void testDistinctFromTopic() {
        final String testName = "test";
        Assert.assertNotEquals(new QueueSource(testName),
                               new TopicSource(testName));
    }
}
