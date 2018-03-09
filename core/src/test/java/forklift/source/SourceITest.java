package forklift.source;

import forklift.source.decorators.Queue;
import forklift.source.decorators.Queues;
import forklift.source.decorators.Topic;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

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

public class SourceITest {
    /**
     * Test creating SourceI lists from annotated consumer classes
     */
    
    public void testCreationFromNoSourceConsumer() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(NoSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList();

        Assert.assertEquals(expectedSources, sources);
    }

    
    public void testCreationFromSingleSourceConsumer() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(SingleSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList(new QueueSource("b"));

        Assert.assertEquals(expectedSources, sources);
    }

    
    public void testCreationFromRepeatedSourceConsumer() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(RepeatedSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList(new QueueSource("a"),
                                                            new QueueSource("b"));

        Assert.assertEquals(expectedSources, sources);
    }


    
    public void testCreationFromManualRepeatedSourceConsumer() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(ManualRepeatedSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList(new QueueSource("a"),
                                                            new QueueSource("b"));

        Assert.assertEquals(expectedSources, sources);
    }

    
    public void testCreationFromMixedSourceConsumer() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(MixedSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList(new QueueSource("test-queue"),
                                                            new TopicSource("test-topic"));

        Assert.assertEquals(expectedSources, sources);
    }

    
    public void testCreationIgnoresNonSourceTypeAnnotations() {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(SomeIrrelevantAnnotationSourceConsumer.class);
        final List<SourceI> expectedSources = Arrays.asList(new QueueSource("a"),
                                                            new TopicSource("b"));

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
     * Test case handling on SourceI
     */
    
    public void testFunctionApplicationNormalCases() {
        final SourceI queueSource = new QueueSource("a");
        final SourceI topicSource = new TopicSource("b");

        Assert.assertEquals("queue-a", simpleSourceOp(queueSource));
        Assert.assertEquals("topic-b", simpleSourceOp(topicSource));
    }

    private String simpleSourceOp(SourceI source) {
        return source
            .apply(QueueSource.class, queue -> "queue-" + queue.getName())
            .apply(TopicSource.class, topic -> "topic-" + topic.getName())
            .get();
    }

    
    public void testFunctionApplicationCaseOrder() {
        final SourceI source = new QueueSource("a");

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

    
    public void testFunctionApplicationUnhandledNull() {
        final SourceI source = new QueueSource("a");

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .get();

        Assert.assertNull(result);
    }

    
    public void testGetOrDefaultAfterUhandledFunctionApplicationGivesDefaultValue() {
        final SourceI source = new QueueSource("a");
        final String defaultValue = "default";

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .getOrDefault(defaultValue);

        Assert.assertEquals(defaultValue, result);
    }

    
    public void testUnhandledFunctionApplicationGivesUnhandledException() {
        final SourceI source = new QueueSource("a");

        final String result = source
            .apply(TopicSource.class, topic -> "topic")
            .elseUnsupportedError();
    }

    
    public void testHandledFunctionApplicationGivesNoUnhandledException() {
        final SourceI source = new QueueSource("a");

        final String result = source
            .apply(QueueSource.class, topic -> "queue")
            .elseUnsupportedError();
    }

    
    public void testVoidFunctionApplicationNormalCases() {
        final AtomicReference<String> state = new AtomicReference<>("unset");
        final SourceI queueSource = new QueueSource("a");
        final SourceI topicSource = new TopicSource("b");

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

    
    public void testVoidFunctionApplicationCaseOrder() {
        final AtomicReference<String> state = new AtomicReference<>("unset");
        final SourceI source = new QueueSource("a");

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

    
    public void testVoidFunctionApplicationGivesNull() {
        final SourceI source = new QueueSource("a");

        Object o = source
            .accept(QueueSource.class, queue -> noop())
            .get();

        Assert.assertNull(o);
    }

    
    public void testUnhandledVoidFunctionApplicationGivesUnhandledException() {
        final SourceI source = new QueueSource("a");

        source
            .accept(TopicSource.class, topic -> noop())
            .elseUnsupportedError();
    }

    class JustATestException extends Exception { }

    
    public void testExceptionalFunctionApplicationThrowsCorrectException() throws JustATestException {
        final SourceI source = new QueueSource("a");

        source.apply(QueueSource.class, queue -> {
            throw new JustATestException();
        });
    }
}
