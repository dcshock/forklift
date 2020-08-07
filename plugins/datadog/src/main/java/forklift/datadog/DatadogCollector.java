package forklift.datadog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * A plug-in that allows tracking portions of the forklift lifecycle directly in Datadog.
 *
 * @author David Thompson
 *
 * Create a datadog metric named trace.forklift.count
 *
 * Include tags: consumer-name, lifecycle, environment, host
 *
 * Need to be able to turn on and off certain lifecycle events with flags
 * Need to be able to pass in credentials via the environment
 *
 */
public class DatadogCollector {
    MeterRegistry registry;

    private Map<String, Map<String, Counter>> counters = new HashMap<>();
    private List<Tag> stdTags;

    public DatadogCollector() {
        registry =new SimpleMeterRegistry();
        stdTags = new ArrayList<>();
        stdTags.add(Tag.of("env", "production"));
        stdTags.add(Tag.of("host", "localhost"));
    }

    @LifeCycle(value=ProcessStep.Pending)
    public void pending(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "pending");
    }

    @LifeCycle(value=ProcessStep.Validating)
    public void validating(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "validating");
    }

    @LifeCycle(value=ProcessStep.Invalid)
    public void invalid(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "invalid");
    }

    @LifeCycle(value=ProcessStep.Processing)
    public void processing(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "processing");
    }

    @LifeCycle(value=ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "complete");
    }

    @LifeCycle(value=ProcessStep.Error)
    public void error(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "error");
    }

    @LifeCycle(value=ProcessStep.Retrying)
    public void retry(MessageRunnable mr) {
        increment(getQueueName(mr.getConsumer().getName()), "retry");
    }

    /**
     * Increment from a map of counters the micrometer counter.
     * @param queue - The name of the queue, topic, stream, etc.
     * @param lifecycle - The lifecycle step in forklift
     * @return - the current counter value for that queue/lifecycle
     */
    protected double increment(String queue, String lifecycle) {
        if (queue == null || queue.length() == 0)
            return 0.0;
        if (lifecycle == null || lifecycle.length() == 0)
            return 0.0;

        Map<String, Counter> queueCounters = counters.get(queue);
        if (queueCounters == null) {
            queueCounters = new HashMap<>();
            counters.put(queue, queueCounters);
        }
        Counter counter = queueCounters.get(lifecycle);
        if (counter == null) {
            counter = Counter.builder("trace.forklift.count").tags(stdTags).
                    tags("queue", queue, "lifecycle", lifecycle).register(registry);
            queueCounters.put(lifecycle, counter);
        }
        counter.increment();
        return counter.count();
    }

    /**
     * Given a queue name which includes the id, strip off the id.
     * @param consumerName as defined by a MessageRunner
     * @return just the consumer-name
     */
    protected static String getQueueName(String consumerName) {
        String cn = consumerName;
        if (cn == null)
            return "";
        int lio = consumerName.lastIndexOf(':');
        if (lio > -1)
            cn = consumerName.substring(0, lio);
        return cn;
    }
}
