package forklift.source;

import org.junit.Assert;
import org.junit.Test;

public class TopicSourceTest {
    @Test
    public void testSameTopicsAreEqual() {
        final String testTopic = "test-topic";

        Assert.assertEquals(new TopicSource(testTopic),
                            new TopicSource(testTopic));
    }

    @Test
    public void testDifferentTopicsAreNotEqual() {
        Assert.assertNotEquals(new TopicSource("test-topic-1"),
                               new TopicSource("test-topic-2"));
    }
}
