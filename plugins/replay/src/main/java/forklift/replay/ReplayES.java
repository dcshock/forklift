package forklift.replay;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.ForkliftSerializer;
import forklift.consumer.Consumer;
import forklift.consumer.ConsumerService;
import forklift.consumer.ConsumerThread;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.consumer.wrapper.RoleInputMessage;
import forklift.decorators.BeanResolver;
import forklift.decorators.LifeCycle;
import forklift.decorators.Service;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import forklift.source.ActionSource;
import forklift.source.SourceI;
import forklift.source.SourceUtil;
import forklift.source.decorators.Topic;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.TopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ReplayES {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final Node node;
    private final ReplayESWriter writer;
    private final ForkliftProducerI producer;
    private final ConsumerThread thread;
    private final Consumer consumer;

    public ReplayES(boolean clientOnly, String hostname, String clusterName, ForkliftConnectorI connector) {
        this(clientOnly, hostname, 9200, clusterName, connector);
    }

    public ReplayES(boolean clientOnly, String hostname, int port, String clusterName, ForkliftConnectorI connector) {
        /*
         * Setup the connection to the server. If we are only a client we'll not setup a node locally to run.
         * This will help developers and smaller setups avoid the pain of setting up elastic search.
         */
        if (clientOnly) {
            node = null;
        } else {
            node = NodeBuilder.nodeBuilder()
                .client(clientOnly)
                .settings(Settings.settingsBuilder().put("http.enabled", true))
                .settings(Settings.settingsBuilder().put("http.cors.enabled", true))
                .settings(Settings.settingsBuilder().put("http.cors.allow-origin", "*"))
                .settings(Settings.settingsBuilder().put("path.home", "."))
                .node();
            node.start();

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException ignored) {
            }
        }

        this.writer = new ReplayESWriter(hostname, port, clusterName);
        this.writer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (node != null && !node.isClosed())
                    node.close();
            }
        });

        final Forklift forklift = new Forklift();
        forklift.setConnector(connector);

        final List<SourceI> sources = SourceUtil.getSourcesAsList(ReplayConsumer.class);
        final SourceI primarySource = sources.stream()
            .filter(source -> !source.isLogicalSource())
            .findFirst().get();

        this.producer = connector.getTopicProducer(primarySource
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

        this.writer.shutdown();
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

    private String fallbackRole(Replay replay, Class<?> msgHandler) {
        return Optional.of(replay.role())
            .filter(role -> !role.isEmpty())
            .orElse(msgHandler.getSimpleName());
    }

    public void msg(MessageRunnable mr, Replay replay, ProcessStep step) {
        final ForkliftMessage msg = mr.getMsg();
        final String id = msg.getId();
        final Map<String, String> props = msg.getProperties();
        final ForkliftConnectorI connector = mr.getConsumer().getForklift().getConnector();
        final Map<String, String> fields = new HashMap<>();

        // Integrate a little with retries. This will stop things from reporting as an error in replay logging for
        // messages that can be retried.
        if (step == ProcessStep.Error &&
            props.containsKey("forklift-retry-count") && !props.containsKey("forklift-retry-max-retries-exceeded")) {
            step = ProcessStep.Retrying;
            mr.setWarnOnly(true);
        }

        Optional<ForkliftSerializer> serializer = Optional.empty();
        if (connector instanceof ForkliftSerializer) {
            serializer = Optional.of((ForkliftSerializer) connector);
        }

        final String connectorName = connector.getClass().getSimpleName();
        final String sourceDescription = mr.getConsumer().getSource().toString();
        final Optional<String> errors = mr.getErrors().stream().reduce((a, b) -> a + ":" + b);

        final Optional<RoleInputSource> declaredRoleSource = mr.getConsumer().getRoleSources(RoleInputSource.class).findFirst();
        final String role = declaredRoleSource.map(source -> source.getRole())
            .orElseGet(() -> fallbackRole(replay, mr.getConsumer().getMsgHandler()));
        final RoleInputSource roleSource = declaredRoleSource.orElseGet(() -> new RoleInputSource(role));
        final ActionSource actionSource = roleSource.getActionSource(connector);

        final RoleInputMessage roleMessage = RoleInputMessage.fromForkliftMessage(role, msg);

        // remove properties that are related to one lifecycle of the message
        final Map<String, String> roleProperties = roleMessage.getProperties();
        roleProperties.remove("forklift-retry-count");
        roleProperties.remove("forklift-retry-max-retries");
        roleProperties.remove("forklift-retry-timeout");
        roleProperties.remove("forklift-retry-max-retries-exceeded");

        final String roleMessageJson = roleMessage.toString();

        // Map in headers
        for (Header key : msg.getHeaders().keySet()) {
            // Skip the correlation id because it is already set in the user id field.
            if (key == Header.CorrelationId)
                continue;

            final Object val = msg.getHeaders().get(key);
            if (val != null)
                fields.put(key.toString(), msg.getHeaders().get(key).toString());
        }

        // carry a description of the original source across restarts
        props.putIfAbsent("source-description", sourceDescription);

        final long stepCount = Integer.parseInt(props.getOrDefault("forklift-replay-step-count", "0")) + 1;
        props.put("forklift-replay-step-count", "" + stepCount);

        // Map in properties
        for (String key : msg.getProperties().keySet()) {
            final Object val = msg.getProperties().get(key);
            if (val != null)
                fields.put(key.toString(), msg.getProperties().get(key).toString());
        }

        // process-level details
        fields.put("role", role);
        fields.put("step", step.toString());
        errors.ifPresent(errorString -> fields.put("errors", errorString));
        fields.put("time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        fields.put("forklift-replay-version", "2");

        // message-level details
        fields.put("text", msg.getMsg());

        // basic details for sending retries
        fields.put("destination-connector", connectorName);
        fields.put("destination-type", actionSource
            .apply(QueueSource.class, queue -> "queue")
            .apply(TopicSource.class, topic -> "topic")
            .apply(GroupedTopicSource.class, topic -> "topic")
            .getOrDefault("unknown")
        );
        fields.put("destination-name", actionSource
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .getOrDefault("unknown")
        );

        // the message to use for resending the original message
        if (serializer.isPresent()) {
            final byte[] roleMessageBytes = serializer.get().serializeForSource(roleSource, roleMessageJson);
            final String roleMessageBase64 = Base64.getEncoder().encodeToString(roleMessageBytes);

            fields.put("destination-message", roleMessageBase64);
            fields.put("destination-message-format", "base64-bytes");
        } else {
            fields.put("destination-message", roleMessageJson);
            fields.put("destination-message-format", "raw-string");
        }

        if (id != null) {
            // Push the message to the consumer
            try {
                this.producer.send(new ReplayESWriterMsg(id, fields, stepCount));
            } catch (ProducerException e) {
                log.error("Unable to produce ES msg", e);
            }
        }
    }
}
