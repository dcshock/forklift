package forklift.replay;

import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.ActionSource;
import forklift.source.SourceI;
import forklift.source.SourceUtil;
import forklift.source.decorators.Topic;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.TopicSource;
import forklift.source.sources.QueueSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * A plugin that writes replay log messages to elasticsearch, so that messages
 * can be reliably tracked and resent.
 *
 * <p>Replay logs are queued on the "forklift.replay.es" queue on the given connector
 * to avoid long delays as messages are being processed. This is handled by a
 * separate consumer which will need to be started for some server.
 *
 * @see ReplayServer
 */
public class ReplayES {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final ForkliftProducerI producer;

    /**
     * Constructs a new ReplayES instance that sends replay mssages to a replay queue.
     *
     * @param connector the connector to use for queuing messages
     */
    public ReplayES(ForkliftConnectorI connector) {
        final List<SourceI> sources = SourceUtil.getSourcesAsList(ReplayConsumer.class);
        final SourceI primarySource = sources.stream()
            .filter(source -> !source.isLogicalSource())
            .findFirst().get();

        this.producer = connector.getQueueProducer(primarySource
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .elseUnsupportedError());
    }

    /* shutdown method retained for comptability and flexibility */
    public void shutdown() {}

    @LifeCycle(value=ProcessStep.Pending, annotation=Replay.class)
    public void pending(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Pending);
    }

    @LifeCycle(value=ProcessStep.Validating, annotation=Replay.class)
    public void validating(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Validating);
    }

    @LifeCycle(value=ProcessStep.Invalid, annotation=Replay.class)
    public void invalid(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Invalid);
    }

    @LifeCycle(value=ProcessStep.Processing, annotation=Replay.class)
    public void processing(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Processing);
    }

    @LifeCycle(value=ProcessStep.Complete, annotation=Replay.class)
    public void complete(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Complete);
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Replay.class)
    public void error(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Error);
    }

    @LifeCycle(value=ProcessStep.Retrying, annotation=Replay.class)
    public void retry(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.Retrying);
    }

    @LifeCycle(value=ProcessStep.MaxRetriesExceeded, annotation=Replay.class)
    public void maxRetries(MessageRunnable mr, Replay replay) {
        msg(mr, replay, ProcessStep.MaxRetriesExceeded);
    }

    public void msg(MessageRunnable mr, Replay replay, ProcessStep step) {
        final ForkliftMessage msg = mr.getMsg();
        final String id = msg.getId();
        final Map<String, String> props = msg.getProperties();
        final ForkliftConnectorI connector = mr.getConsumer().getForklift().getConnector();

        // Integrate a little with retries. This will stop things from reporting as an error in replay logging for
        // messages that can be retried.
        if (step == ProcessStep.Error &&
            props.containsKey("forklift-retry-count") && !props.containsKey("forklift-retry-max-retries-exceeded")) {
            step = ProcessStep.Retrying;
            mr.setWarnOnly(true);
        }

        if (id != null && !id.isEmpty()) {
            ReplayLogBuilder logBuilder = new ReplayLogBuilder(msg, mr.getConsumer(), mr.getErrors(), connector, replay, step);
            // Push the message to the consumer
            try {
                this.producer.send(new ReplayESWriterMsg(id, logBuilder.getFields(), logBuilder.getStepCount()));
            } catch (ProducerException e) {
                log.error("Unable to produce ES msg", e);
            }
        }
    }
}
