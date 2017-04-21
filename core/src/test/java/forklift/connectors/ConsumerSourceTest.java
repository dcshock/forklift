package forklift.connectors;

import forklift.decorators.Queue;
import forklift.decorators.Queues;
import forklift.decorators.Topic;
import forklift.source.QueueSource;
import forklift.source.TopicSource;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ConsumerSourceTest {
    @Test
    public void testSameSourceEqual() {
        Assert.assertEquals(new ConsumerSource(new QueueSource("test")),
                            new ConsumerSource(new QueueSource("test")));
    }

    @Test
    public void testDifferentSourceNotEqual() {
        Assert.assertNotEquals(new ConsumerSource(new QueueSource("test")),
                               new ConsumerSource(new TopicSource("test")));
    }

    @Test
    public void testCreationFromNoSourceConsumer() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(NoSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList();

        Assert.assertEquals(expectedSources, sources);
    }

    @Test
    public void testCreationFromSingleSourceConsumer() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(SingleSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList(new ConsumerSource(new QueueSource("b")));

        Assert.assertEquals(expectedSources, sources);
    }

    @Test
    public void testCreationFromRepeatedSourceConsumer() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(RepeatedSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList(new ConsumerSource(new QueueSource("a")),
                                                                   new ConsumerSource(new QueueSource("b")));

        Assert.assertEquals(expectedSources, sources);
    }

    @Test
    public void testCreationFromManualRepeatedSourceConsumer() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(ManualRepeatedSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList(new ConsumerSource(new QueueSource("a")),
                                                                   new ConsumerSource(new QueueSource("b")));

        Assert.assertEquals(expectedSources, sources);
    }

    @Test
    public void testCreationFromMixedSourceConsumer() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(MixedSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList(new ConsumerSource(new QueueSource("test-queue")),
                                                                   new ConsumerSource(new TopicSource("test-topic")));

        Assert.assertEquals(expectedSources, sources);
    }

    class NoSourceConsumer {}

    @Queue("b")
    class SingleSourceConsumer {}

    @Queue("a")
    @Queue("b")
    class RepeatedSourceConsumer {}

    @Queues({
        @Queue("a"),
        @Queue("b")
    })
    class ManualRepeatedSourceConsumer {}

    @Queue("test-queue")
    @Topic("test-topic")
    class MixedSourceConsumer {}

}
