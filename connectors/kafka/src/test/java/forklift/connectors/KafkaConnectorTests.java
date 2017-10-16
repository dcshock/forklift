package forklift.connectors;

import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaConnectorTests {
    private static final String GROUP_ID = "test-default";
    private KafkaConnector connector;

    @Before
    public void setup() throws Exception {
        connector = new KafkaConnector("blah", "blah", GROUP_ID);
    }

    @Test
    public void testQueueMapping() {
        final String topicName = "test-topic";

        final GroupedTopicSource mappedSource = connector.mapToGroupedTopic(new QueueSource(topicName));
        Assert.assertEquals(new GroupedTopicSource(topicName, GROUP_ID), mappedSource);
    }

    @Test
    public void testTopicMapping() {
        final String topicName = "test-topic";
        final TopicSource consumerSource = new TopicSource(topicName);
        consumerSource.setContextClass(FakeConsumer.class);

        final GroupedTopicSource mappedConsumerSource = connector.mapToGroupedTopic(consumerSource);
        Assert.assertEquals(new GroupedTopicSource(topicName, "test-default-FakeConsumer"), mappedConsumerSource);
    }

    @Test
    public void testDefaultTopicMapping() {
        final String topicName = "test-topic";
        final TopicSource anonymousSource = new TopicSource(topicName);

        final GroupedTopicSource mappedConsumerSource = connector.mapToGroupedTopic(anonymousSource);
        Assert.assertEquals(new GroupedTopicSource(topicName, GROUP_ID), mappedConsumerSource);
    }

    @Test
    public void testGroupedTopicMapping() {
        final String topicName = "test-topic";
        final String groupId = "test-group";
        final GroupedTopicSource unmappedSource = new GroupedTopicSource(topicName, groupId);

        final GroupedTopicSource mappedSource = connector.mapToGroupedTopic(unmappedSource);
        Assert.assertEquals(unmappedSource, mappedSource);
    }

    private final class FakeConsumer {}
}
