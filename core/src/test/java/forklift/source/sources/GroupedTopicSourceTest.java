package forklift.source.sources;

import org.junit.Assert;
import org.junit.Test;

public class GroupedTopicSourceTest {
    
    public void testEmptyOrNullGroupIsUnspecified() {
        Assert.assertFalse(new GroupedTopicSource("test-topic", null).groupSpecified());
        Assert.assertFalse(new GroupedTopicSource("test-topic", "").groupSpecified());
    }

    
    public void testNonEmptyGroupIsSpecified() {
        Assert.assertTrue(new GroupedTopicSource("test-topic", "test-group").groupSpecified());
    }

    
    public void testGroupIsOverriden() {
        final String testGroup = "test-group";
        final GroupedTopicSource source = new GroupedTopicSource("test-topic", testGroup);
        Assert.assertEquals(testGroup, source.getGroup());

        final String newGroup = "new-group";
        source.overrideGroup(newGroup);
        Assert.assertEquals(newGroup, source.getGroup());
    }

    
    public void testEqualsWorksNormally() {
        final String testTopic = "test-topic";
        final String testGroup = "test-group";
        Assert.assertEquals(new GroupedTopicSource(testTopic, testGroup),
                            new GroupedTopicSource(testTopic, testGroup));
    }

    
    public void testDifferentSourcesAreNotEqual() {
        final String testTopic = "test-topic";
        final String testGroup = "test-group";
        final String otherTopic = "other-topic";

        Assert.assertNotEquals(new GroupedTopicSource(testTopic, testGroup),
                               new GroupedTopicSource(otherTopic, testGroup));

        Assert.assertNotEquals(new GroupedTopicSource(testTopic, testGroup),
                               new TopicSource(testTopic));
    }
}
