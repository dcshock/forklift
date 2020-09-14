package forklift.source.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

public class TopicSourceTest {
    @Test
    public void testSameTopicsAreEqual() {
        final String testTopic = "test-topic";

        assertEquals(new TopicSource(testTopic),
                            new TopicSource(testTopic));
    }

    @Test
    public void testDifferentTopicsAreNotEqual() {
        assertNotEquals(new TopicSource("test-topic-1"),
                               new TopicSource("test-topic-2"));
    }
}
