package forklift.datadog;

import java.time.Duration;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.lang.Nullable;
import io.micrometer.datadog.DatadogConfig;
import io.micrometer.datadog.DatadogMeterRegistry;

/**
 * A plug-in that allows tracking portions of the forklift lifecycle directly in Datadog.
 *
 * @author David Thompson
 *
 * Create datadog metrics named trace.forklift.count trace.forklift.timer
 *
 * Include tags: consumer-name, lifecycle, environment, host
 *
 */
public class DatadogCollector extends SimpleCollector {
    private static final String DATADOG_LIFECYCLE_COUNTER = "datadog.lifecycle.counter";
    private static final String DATADOG_LIFECYCLE_TIMER = "datadog.lifecycle.timer";
    private Logger log = LoggerFactory.getLogger(DatadogCollector.class);

    public DatadogCollector(String apiKey, String applicationKey, String environment, String host) {
        if (apiKey == null || apiKey.length() == 0)
            throw new IllegalArgumentException("Datadog API key cannot be null");

        DatadogConfig config = new DatadogConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(10);
            }
            @Override
            public String get(String k) {
                return null;
            }
            @Override
            public String apiKey() {
                return apiKey;
            }
            @Override
            @Nullable
            public String applicationKey() {
                return applicationKey;
            }
        };
        registry = new DatadogMeterRegistry(config, Clock.SYSTEM);
        stdTags = new ArrayList<>();
        if (environment != null && environment.length() > 0)
            stdTags.add(Tag.of("env", environment));
        if (host != null && host.length() > 0)
            stdTags.add(Tag.of("host", host));
        log.info("DatadogCollector created");
    }

    // Annotations can not be inherited at the method level. Requires
    // the lifecycle injection to exist at the instantiated class level
    // Stupid as this is the same exact methods as the super class

    @LifeCycle(value=ProcessStep.Pending)
    public void pending(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "pending", DATADOG_LIFECYCLE_COUNTER);
    }

    @LifeCycle(value=ProcessStep.Validating)
    public void validating(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "validating", DATADOG_LIFECYCLE_COUNTER);
    }

    @LifeCycle(value=ProcessStep.Invalid)
    public void invalid(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "invalid", DATADOG_LIFECYCLE_COUNTER);
    }

    @LifeCycle(value=ProcessStep.Processing)
    public void processing(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "processing", DATADOG_LIFECYCLE_COUNTER);
        timerStart(getConsumerName(mr.getConsumer().getName()), "processing", DATADOG_LIFECYCLE_TIMER);
    }

    @LifeCycle(value=ProcessStep.Complete)
    public void complete(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "complete", DATADOG_LIFECYCLE_COUNTER);
        timerStop(getConsumerName(mr.getConsumer().getName()), "processing", DATADOG_LIFECYCLE_TIMER);
    }

    @LifeCycle(value=ProcessStep.Error)
    public void error(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "error", DATADOG_LIFECYCLE_COUNTER);
        timerStop(getConsumerName(mr.getConsumer().getName()), "processing", DATADOG_LIFECYCLE_TIMER);
    }

    @LifeCycle(value=ProcessStep.Retrying)
    public void retry(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "retry", DATADOG_LIFECYCLE_COUNTER);
    }

    @LifeCycle(value=ProcessStep.MaxRetriesExceeded)
    public void maxRetries(MessageRunnable mr) {
        increment(getConsumerName(mr.getConsumer().getName()), "max-retries", DATADOG_LIFECYCLE_COUNTER);
    }
}
