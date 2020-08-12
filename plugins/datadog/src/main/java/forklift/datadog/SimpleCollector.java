package forklift.datadog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * A simple micrometer meter incrementor for counting lifecycle events.
 *
 * @author David Thompson
 *
 */
public class SimpleCollector {
    MeterRegistry registry;

    private Logger log = LoggerFactory.getLogger(SimpleCollector.class);
    private Map<String, Map<String, Counter>> counters = new HashMap<>();
    protected List<Tag> stdTags;

    public SimpleCollector() {
        registry = new SimpleMeterRegistry();
        log.info("DatadogCollector created");
    }

    @LifeCycle(value=ProcessStep.Pending)
    public void pending(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "pending");
    }

    @LifeCycle(value=ProcessStep.Validating)
    public void validating(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "validating");
    }

    @LifeCycle(value=ProcessStep.Invalid)
    public void invalid(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "invalid");
    }

    @LifeCycle(value=ProcessStep.Processing)
    public void processing(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "processing");
    }

    @LifeCycle(value=ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "complete");
    }

    @LifeCycle(value=ProcessStep.Error)
    public void error(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "error");
    }

    @LifeCycle(value=ProcessStep.Retrying)
    public void retry(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "retry");
    }

    @LifeCycle(value=ProcessStep.MaxRetriesExceeded)
    public void maxRetries(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "max-retries");
    }

    /**
     * Increment from a map of counters the micrometer counter.
     * @param consumerName - The name of the queue, topic, stream, etc.
     * @param lifecycle - The lifecycle step in forklift
     * @return - the current counter value for that queue/lifecycle
     */
    protected double increment(String consumerName, String lifecycle) {
        if (consumerName == null || consumerName.length() == 0)
            return 0.0;
        if (lifecycle == null || lifecycle.length() == 0)
            return 0.0;

        Map<String, Counter> queueCounters = counters.get(consumerName);
        if (queueCounters == null) {
            queueCounters = new HashMap<>();
            counters.put(consumerName, queueCounters);
        }
        Counter counter = queueCounters.get(lifecycle);
        if (counter == null) {
            counter = Counter.builder("trace.forklift.count").tags(stdTags).
                    tags("consumer-name", consumerName, "lifecycle", lifecycle).register(registry);
            queueCounters.put(lifecycle, counter);
        }
        counter.increment();
        return counter.count();
    }

    /**
     * Given a consumer name which includes the id, strip off the id.
     * @param consumerName as defined by a MessageRunner
     * @return just the consumer-name
     */
    protected static String getConsumerName(String consumerName) {
        String cn = consumerName;
        if (cn == null)
            return "";
        int lio = consumerName.lastIndexOf(':');
        if (lio > -1)
            cn = consumerName.substring(0, lio);
        return cn;
    }

}
