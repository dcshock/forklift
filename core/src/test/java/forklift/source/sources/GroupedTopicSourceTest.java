package forklift.source.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class GroupedTopicSourceTest {
    @Test
    public void testEmptyOrNullGroupIsUnspecified() {
        assertFalse(new GroupedTopicSource("test-topic", null).groupSpecified());
        assertFalse(new GroupedTopicSource("test-topic", "").groupSpecified());
    }

    @Test
    public void testNonEmptyGroupIsSpecified() {
        assertTrue(new GroupedTopicSource("test-topic", "test-group").groupSpecified());
    }

    @Test
    public void testGroupIsOverriden() {
        final String testGroup = "test-group";
        final GroupedTopicSource source = new GroupedTopicSource("test-topic", testGroup);
        assertEquals(testGroup, source.getGroup());

        final String newGroup = "new-group";
        source.overrideGroup(newGroup);
        assertEquals(newGroup, source.getGroup());
    }

    @Test
    public void testEqualsWorksNormally() {
        final String testTopic = "test-topic";
        final String testGroup = "test-group";
        assertEquals(new GroupedTopicSource(testTopic, testGroup),
                            new GroupedTopicSource(testTopic, testGroup));
    }

    @Test
    public void testDifferentSourcesAreNotEqual() {
        final String testTopic = "test-topic";
        final String testGroup = "test-group";
        final String otherTopic = "other-topic";

        assertNotEquals(new GroupedTopicSource(testTopic, testGroup),
                               new GroupedTopicSource(otherTopic, testGroup));

        assertNotEquals(new GroupedTopicSource(testTopic, testGroup),
                               new TopicSource(testTopic));
    }
}
