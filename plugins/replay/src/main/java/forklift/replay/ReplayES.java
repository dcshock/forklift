package forklift.replay;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.ConsumerService;
import forklift.consumer.ConsumerThread;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.BeanResolver;
import forklift.decorators.LifeCycle;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.SourceI;
import forklift.source.SourceUtil;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.TopicSource;
import forklift.source.sources.QueueSource;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A plugin that writes replay log messages to elasticsearch, so that messages
 * can be reliably tracked and resent.
 *
 * Replay logs are queued on the "forklift.replay.es" queue on the given connector
 * to avoid long delays as messages are being processed.
 */
public class ReplayES {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final Node node;
    private final ReplayESWriter writer;
    private final ForkliftProducerI producer;
    private final ConsumerThread thread;
    private final Consumer consumer;

    /**
     * Constructs a new ReplayES instance using the default REST port (9200) for
     * sending messages to elasticsearch.
     *
     * @param clientOnly whether the plugin is only an elasticsearch client, or an
     *     embedded elasticsearch node should be started
     * @param hostname the address of the ES host to send messages to
     * @param clusterName the name of the elasticsearch cluster to send logs to
     * @param connector the connector to use for queuing messages
     *
     * @deprecated use {@link #ReplayES(boolean, String, ForkliftConnectorI)} instead
     */
    @Deprecated
    public ReplayES(boolean clientOnly, String hostname, String clusterName, ForkliftConnectorI connector) {
        this(clientOnly, hostname, 9200, clusterName, connector);
    }

    /**
     * Constructs a new ReplayES instance sending messages to elasticsearch based
     * on the given parameters.
     *
     * @param clientOnly whether the plugin is only an elasticsearch client, or an
     *     embedded elasticsearch node should be started
     * @param hostname the address of the ES host to send messages to
     * @param port the port number of the ES transport port for the given host
     * @param clusterName the name of the elasticsearch cluster to send logs to
     * @param connector the connector to use for queuing messages
     *
     * @deprecated use {@link #ReplayES(boolean, String, int, ForkliftConnectorI)} instead
     */
    @Deprecated
    public ReplayES(boolean clientOnly, String hostname, int port, String clusterName, ForkliftConnectorI connector) {
        this(clientOnly, hostname, port, connector);
    }

    /**
     * Constructs a new ReplayES instance using the default REST port (9200) for
     * sending messages to elasticsearch.
     *
     * @param clientOnly whether the plugin is only an elasticsearch client, or an
     *     embedded elasticsearch node should be started
     * @param hostname the address of the ES host to send messages to
     * @param connector the connector to use for queuing messages
     */
    public ReplayES(boolean clientOnly, String hostname, ForkliftConnectorI connector) {
        this(clientOnly, hostname, 9200, connector);
    }


    /**
     * Constructs a new ReplayES instance sending messages to elasticsearch based
     * on the given parameters.
     *
     * @param clientOnly whether the plugin is only an elasticsearch client, or an
     *     embedded elasticsearch node should be started
     * @param hostname the address of the ES host to send messages to
     * @param port the port number of the ES transport port for the given host
     * @param connector the connector to use for queuing messages
     */
    public ReplayES(boolean clientOnly, String hostname, int port, ForkliftConnectorI connector) {
        /*
         * Setup the connection to the server. If we are only a client we'll not setup a node locally to run.
         * This will help developers and smaller setups avoid the pain of setting up elastic search.
         */
        if (clientOnly) {
            node = null;
        } else {
            Settings settings = Settings.builder()
                .put("http.enabled", true)
                .put("http.cors.enabled", true)
                .put("http.cors.allow-origin", "*")
                .put("path.home", ".")
                .build();
                node = new Node(settings);

            try {
                node.start();
                Thread.sleep(10000L);
            } catch (InterruptedException | NodeValidationException ignored) {
            }
        }

        this.writer = new ReplayESWriter(hostname, port);
        this.writer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (node != null && !node.isClosed())
                    try {
                        node.close();
                    } catch (IOException ignored) {}
            }
        });

        final Forklift forklift = new Forklift();
        forklift.setConnector(connector);

        final List<SourceI> sources = SourceUtil.getSourcesAsList(ReplayConsumer.class);
        final SourceI primarySource = sources.stream()
            .filter(source -> !source.isLogicalSource())
            .findFirst().get();

        this.producer = connector.getQueueProducer(primarySource
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .elseUnsupportedError());
        this.consumer = new Consumer(ReplayConsumer.class, forklift,
            Thread.currentThread().getContextClassLoader(), primarySource, sources);
        this.consumer.addServices(new ConsumerService(this));
        this.thread = new ConsumerThread(this.consumer);
        this.thread.setName("ReplayES");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    @BeanResolver
    public Object resolve(Class<?> c, String name) {
        if (c == ReplayESWriter.class)
            return this.writer;

        return null;
    }

    public void shutdown() {
        this.thread.shutdown();
        try {
            this.thread.join(180 * 1000);
        } catch (InterruptedException ignored) {
        }

        this.writer.close();
    }

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
