package forklift.source.sources;

import org.junit.Assert;
import org.junit.Test;

public class TopicSourceTest {
    
    public void testSameTopicsAreEqual() {
        final String testTopic = "test-topic";

        Assert.assertEquals(new TopicSource(testTopic),
                            new TopicSource(testTopic));
    }

    
    public void testDifferentTopicsAreNotEqual() {
        Assert.assertNotEquals(new TopicSource("test-topic-1"),
                               new TopicSource("test-topic-2"));
    }
}
