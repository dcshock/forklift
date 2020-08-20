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
import io.micrometer.core.instrument.Timer;
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
    private Map<String, Map<String, Timer.Sample>> timers = new HashMap<>();
    protected List<Tag> stdTags;

    public SimpleCollector() {
        registry = new SimpleMeterRegistry();
        log.info("SimpleCollector created");
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
     * @param propValue - The property value if set can turn off a counter
     * @return - the current counter value for that queue/lifecycle
     */
    protected double increment(String consumerName, String lifecycle, String propValue) {
        if (isTurnedOff(consumerName, lifecycle, propValue))
            return 0.0;

        return increment(consumerName, lifecycle);
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

        // Micrometer counters handle creating non-existing or returning existing if
        // it already exists.
        Counter counter = Counter.builder("trace.forklift.count").tags(stdTags).
                    tags("consumer-name", consumerName, "lifecycle", lifecycle).register(registry);
        counter.increment();
        return counter.count();
    }

    /**
     * Use a micrometer sampler to start recording the time it takes to run until stopped.
     * @param consumerName - The name of the queue, topic, stream, etc.
     * @param lifecycle - The lifecycle step in forklift
     * @param propValue - The property value if set can turn off a timer
     * @return the timer sampler created for this consumerName
     */
    protected Timer.Sample timerStart(String consumerName, String lifecycle, String propValue) {
        if (isTurnedOff(consumerName, lifecycle, propValue))
            return null;

        return timerStart(consumerName, lifecycle);
    }

    protected Timer.Sample timerStart(String consumerName, String lifecycle) {
        if (consumerName == null || consumerName.length() == 0)
            return null;
        if (lifecycle == null || lifecycle.length() == 0)
            return null;

        Map<String, Timer.Sample> queueTimers = timers.get(consumerName);
        if (queueTimers == null) {
            queueTimers = new HashMap<>();
            timers.put(consumerName, queueTimers);
        }
        // In order to make this thread-safe, we need to add the thread id.
        // Since the lifecycle in forklift is guaranteed to run on a single thread
        // then we can guarantee that start and stop will live on the same
        // thread, thus the same thread id.
        String threadLifecycle = lifecycle + Thread.currentThread().getId();
        Timer.Sample sampler = queueTimers.get(threadLifecycle);
        if (sampler == null) {
            sampler = Timer.start(registry);
            queueTimers.put(threadLifecycle, sampler);
        }
        return sampler;
    }

    /**
     * Create a micrometer timer and record the time it took from when the sampler started until
     * this method is called to stop it.
     * @param consumerName - The name of the queue, topic, stream, etc.
     * @param lifecycle - The lifecycle step in forklift
     * @param propValue - The property value if set can turn off a timer
     * @return the number of times the timer has been called
     */
    protected double timerStop(String consumerName, String lifecycle, String propValue) {
        if (isTurnedOff(consumerName, lifecycle, propValue))
            return 0.0;

        return timerStop(consumerName, lifecycle);
    }

    protected double timerStop(String consumerName, String lifecycle) {
        if (consumerName == null || consumerName.length() == 0)
            return 0.0;
        if (lifecycle == null || lifecycle.length() == 0)
            return 0.0;

        Map<String, Timer.Sample> queueSamplers = timers.get(consumerName);
        if (queueSamplers == null) {
            // timer never started
            return 0.0;
        }
        // In order to make this thread-safe, we need to add the thread id.
        String threadLifecycle = lifecycle + Thread.currentThread().getId();
        Timer.Sample sampler = queueSamplers.get(threadLifecycle);
        if (sampler == null) {
            // timer never started
            return 0.0;
        }
        Timer timer = Timer.builder("trace.forklift.timer").tags(stdTags).
                tags("consumer-name", consumerName, "lifecycle", lifecycle).register(registry);
        sampler.stop(timer);
        // Cleanup the stopped timer.
        queueSamplers.remove(threadLifecycle);

        return timer.count();
    }

    /**
     * Given a consumer name which includes the id, strip off the id.
     * @param consumerName as defined by a MessageRunner
     * @return just the consumer-name
     */
    static String getConsumerName(String consumerName) {
        String cn = consumerName;
        if (cn == null)
            return "";
        int lio = consumerName.lastIndexOf(':');
        if (lio > -1)
            cn = consumerName.substring(0, lio);
        return cn;
    }

    boolean isTurnedOff(String consumerName, String lifecycle, String propValue) {
        String lifecycleProp = System.getProperty(propValue + "." + lifecycle);
        String consumerProp = System.getProperty(propValue + "." + lifecycle + "." + consumerName );

        if (consumerProp != null && !Boolean.parseBoolean(consumerProp))
            return true;

        if (consumerProp == null && lifecycleProp != null && !Boolean.parseBoolean(lifecycleProp))
            return true;

        return false;
    }
}
