package forklift.replay;

import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.consumer.ConsumerService;
import forklift.consumer.ConsumerThread;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.BeanResolver;
import forklift.decorators.LifeCycle;
import forklift.decorators.Queue;
import forklift.decorators.Service;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.jms.JMSException;

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

        this.producer = connector.getQueueProducer(ReplayConsumer.class.getAnnotation(Queue.class).value());

        this.consumer = new Consumer(ReplayConsumer.class, connector,
            Thread.currentThread().getContextClassLoader(), ReplayConsumer.class.getAnnotation(Queue.class));
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
    public void pending(MessageRunnable mr) {
        msg(mr, ProcessStep.Pending);
    }

    @LifeCycle(value=ProcessStep.Validating, annotation=Replay.class)
    public void validating(MessageRunnable mr) {
        msg(mr, ProcessStep.Validating);
    }

    @LifeCycle(value=ProcessStep.Invalid, annotation=Replay.class)
    public void invalid(MessageRunnable mr) {
        msg(mr, ProcessStep.Invalid);
    }

    @LifeCycle(value=ProcessStep.Processing, annotation=Replay.class)
    public void processing(MessageRunnable mr) {
        msg(mr, ProcessStep.Processing);
    }

    @LifeCycle(value=ProcessStep.Complete, annotation=Replay.class)
    public void complete(MessageRunnable mr) {
        msg(mr, ProcessStep.Complete);
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Replay.class)
    public void error(MessageRunnable mr) {
        msg(mr, ProcessStep.Error);
    }

    @LifeCycle(value=ProcessStep.Retrying, annotation=Replay.class)
    public void retry(MessageRunnable mr) {
        msg(mr, ProcessStep.Retrying);
    }

    @LifeCycle(value=ProcessStep.MaxRetriesExceeded, annotation=Replay.class)
    public void maxRetries(MessageRunnable mr) {
        msg(mr, ProcessStep.MaxRetriesExceeded);
    }

    public void msg(MessageRunnable mr, ProcessStep step) {
        final ForkliftMessage msg = mr.getMsg();

        // Read props of the message to see what we need to do with retry counts
        final Map<String, Object> props = msg.getProperties();

        final Map<String, String> fields = new HashMap<>();
        fields.put("text", msg.getMsg());

        // Integrate a little with retries. This will stop things from reporting as an error in replay logging for
        // messages that can be retried.
        if (step == ProcessStep.Error &&
            props.containsKey("forklift-retry-count") && !props.containsKey("forklift-retry-max-retries-exceeded")) {
            fields.put("step", ProcessStep.Retrying.toString());
            mr.setWarnOnly(true);
        } else {
            fields.put("step", step.toString());
        }

        // Map in headers
        for (Header key : msg.getHeaders().keySet()) {
            // Skip the correlation id because it is already set in the user id field.
            if (key == Header.CorrelationId)
                continue;

            final Object val = msg.getHeaders().get(key);
            if (val != null)
                fields.put(key.toString(), msg.getHeaders().get(key).toString());
        }

        // Map in properties
        for (String key : msg.getProperties().keySet()) {
            final Object val = msg.getProperties().get(key);
            if (val != null)
                fields.put(key.toString(), msg.getProperties().get(key).toString());
        }

        // Errors are nullable.
        final Optional<String> errors = mr.getErrors().stream().reduce((a, b) -> a + ":" + b);
        if (errors.isPresent())
            fields.put("errors", errors.get());

        // Add a timestamp of when we processed this replay message.
        fields.put("time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        // Store the queue/topic.
        if (mr.getConsumer().getQueue() != null)
            fields.put("queue", mr.getConsumer().getQueue().value());
        if (mr.getConsumer().getTopic() != null)
            fields.put("topic", mr.getConsumer().getTopic().value());

        // Generate the id from the correlation id first followed by the generated amq id.
        String id = null;
        try {
            id = msg.getJmsMsg().getJMSCorrelationID();
            if (id == null || "".equals(id))
                id = msg.getJmsMsg().getJMSMessageID();
        } catch (JMSException ignored) {
        }

        // Push the message to the consumer
        try {
            this.producer.send(new ReplayESWriterMsg(id, fields));
        } catch (ProducerException e) {
            log.error("Unable to producer ES msg", e);
        }
    }
}