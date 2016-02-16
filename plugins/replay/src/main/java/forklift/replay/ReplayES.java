package forklift.replay;

import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.message.Header;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.jms.JMSException;

public class ReplayES {
    private final Node node;
    private final ReplayESWriter writer;

    public ReplayES(boolean clientOnly, boolean ssl, String hostname) {
        this(clientOnly, ssl, hostname, 9200);
    }

    public ReplayES(boolean clientOnly, boolean ssl, String hostname, int port) {
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

        this.writer = new ReplayESWriter(ssl, hostname);
        this.writer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (node != null && !node.isClosed())
                    node.close();
            }
        });
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

        final Map<String, String> fields = new HashMap<String, String>();
        fields.put("text", msg.getMsg());
        fields.put("step", step.toString());

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

        // Push the message to the writer.
        writer.put(new ReplayESWriter.ReplayESWriterMsg(id, fields));
    }
}