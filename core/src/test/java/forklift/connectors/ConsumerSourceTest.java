package forklift.connectors;

import forklift.source.QueueSource;
import forklift.source.TopicSource;
import forklift.source.decorators.Queue;
import forklift.source.decorators.Queues;
import forklift.source.decorators.Topic;

import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ConsumerSourceTest {
    @Test
    public void testSameSourceEqual() {
        Assert.assertEquals(new ConsumerSource(new QueueSource("test")),
                            new ConsumerSource(new QueueSource("test")));
    }

    @Test
    public void testSameSourceTypeWithDifferentPropertiesNotEqual() {
        Assert.assertNotEquals(new ConsumerSource(new QueueSource("test")),
                               new ConsumerSource(new QueueSource("not-test")));
    }

    @Test
    public void testDifferentSourceNotEqual() {
        Assert.assertNotEquals(new ConsumerSource(new QueueSource("test")),
                               new ConsumerSource(new TopicSource("test")));
    }

    /**
     * Test creating ConsumerSource lists from annotated consumer classes
     */
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

    @Test
    public void testCreationIgnoresNonSourceTypeAnnotations() {
        final List<ConsumerSource> sources = ConsumerSource.getConsumerSources(SomeIrrelevantAnnotationSourceConsumer.class);
        final List<ConsumerSource> expectedSources = Arrays.asList(new ConsumerSource(new QueueSource("a")),
                                                                   new ConsumerSource(new TopicSource("b")));

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

    // Some meaningless annotations to test that they are ignored
    @Target(ElementType.TYPE) @Retention(RetentionPolicy.RUNTIME) @interface Bogus {}
    @Target(ElementType.TYPE) @Retention(RetentionPolicy.RUNTIME) @Repeatable(Things.class) @interface Thing {}
    @Target(ElementType.TYPE) @Retention(RetentionPolicy.RUNTIME) @interface Things { Thing[] value(); }

    @Queue("a")
    @Bogus
    @Things({
        @Thing,
        @Thing
    })
    @Topic("b")
    class SomeIrrelevantAnnotationSourceConsumer {}

    /**
     * Test case handling on ConsumerSource
     */
    @Test
    public void testFunctionApplicationNormalCases() {
        final ConsumerSource queueSource = new ConsumerSource(new QueueSource("a"));
        final ConsumerSource topicSource = new ConsumerSource(new TopicSource("b"));

        Assert.assertEquals("queue-a", simpleSourceOp(queueSource));
        Assert.assertEquals("topic-b", simpleSourceOp(topicSource));
    }

    private String simpleSourceOp(ConsumerSource source) {
        return source
            .apply(QueueSource.class, queue -> "queue-" + queue.getName())
            .apply(TopicSource.class, topic -> "topic-" + topic.getName())
            .get();
    }

    @Test
    public void testFunctionApplicationCaseOrder() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        final String resultOrderA = source
            .apply(QueueSource.class, queue -> "queue")
            .apply(TopicSource.class, topic -> "topic")
            .get();
        final String resultOrderB = source
            .apply(TopicSource.class, topic -> "topic")
            .apply(QueueSource.class, queue -> "queue")
            .get();

        Assert.assertEquals(resultOrderA, resultOrderB);
    }

    @Test
    public void testFunctionApplicationUnhandledNull() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .get();

        Assert.assertNull(result);
    }

    @Test
    public void testGetOrDefaultAfterUhandledFunctionApplicationGivesDefaultValue() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));
        final String defaultValue = "default";

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .getOrDefault(defaultValue);

        Assert.assertEquals(defaultValue, result);
    }

    @Test(expected = RuntimeException.class)
    public void testUnhandledFunctionApplicationGivesUnhandledException() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .elseUnsupportedError();
    }

    @Test
    public void testHandledFunctionApplicationGivesNoUnhandledException() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        final String result = source
            .apply(QueueSource.class, topic -> "queue")
            .elseUnsupportedError();
    }

    @Test
    public void testVoidFunctionApplicationNormalCases() {
        final AtomicReference<String> state = new AtomicReference<>("unset");
        final ConsumerSource queueSource = new ConsumerSource(new QueueSource("a"));
        final ConsumerSource topicSource = new ConsumerSource(new TopicSource("b"));

        queueSource
            .accept(QueueSource.class, queue -> state.set("queue"))
            .accept(TopicSource.class, topic -> state.set("topic"));
        Assert.assertEquals(state.get(), "queue");

        state.set("unset");

        topicSource
            .accept(QueueSource.class, queue -> state.set("queue"))
            .accept(TopicSource.class, topic -> state.set("topic"));
        Assert.assertEquals(state.get(), "topic");
    }

    @Test
    public void testVoidFunctionApplicationCaseOrder() {
        final AtomicReference<String> state = new AtomicReference<>("unset");
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        source
            .accept(QueueSource.class, queue -> state.set("queue"))
            .accept(TopicSource.class, topic -> state.set("topic"));
        final String stateOrderA = state.get();

        state.set("unset");

        source
            .accept(TopicSource.class, topic -> state.set("topic"))
            .accept(QueueSource.class, queue -> state.set("queue"));
        final String stateOrderB = state.get();

        Assert.assertEquals(stateOrderA, stateOrderB);
    }

    private void noop() {}

    @Test
    public void testVoidFunctionApplicationGivesNull() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        Object o = source
            .accept(QueueSource.class, queue -> noop())
            .get();

        Assert.assertNull(o);
    }

    @Test(expected = RuntimeException.class)
    public void testUnhandledVoidFunctionApplicationGivesUnhandledException() {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        source
            .accept(TopicSource.class, topic -> noop())
            .elseUnsupportedError();
    }

    class JustATestException extends Exception { }

    @Test(expected = JustATestException.class)
    public void testExceptionalFunctionApplicationThrowsCorrectException() throws JustATestException {
        final ConsumerSource source = new ConsumerSource(new QueueSource("a"));

        source.apply(QueueSource.class, queue -> {
            throw new JustATestException();
        });
    }
}
