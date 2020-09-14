package forklift.retry;

import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftSerializer;
import forklift.producers.ForkliftProducerI;
import forklift.source.SourceI;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;

public class RetryRunnable implements Runnable {
    public static final Logger log = LoggerFactory.getLogger(RetryRunnable.class);

    private String id;
    private Map<String, String> fields;
    private ForkliftConnectorI connector;
    private Consumer<String> complete;

    public RetryRunnable(String id, Map<String, String> fields, ForkliftConnectorI connector, Consumer<String> complete) {
        this.id = id;
        this.fields = fields;
        this.connector = connector;
        this.complete = complete;
    }

    @Override
    public void run() {
        ForkliftProducerI producer = null;
        SourceI destinationSource = null;

        try {
            final String destinationType = fields.get("destination-type");
            final String destinationName = fields.get("destination-name");

            if ("queue".equals(destinationType)) {
                producer = connector.getQueueProducer(destinationName);
                destinationSource = new QueueSource(destinationName);
            } else if ("topic".equals(destinationType)) {
                producer = connector.getTopicProducer(destinationName);
                destinationSource = new TopicSource(destinationName);
            }
        } catch (Throwable e) {
            log.error("", e);
            e.printStackTrace();
            return;
        }

        final String messageFormat = fields.get("destination-message-format");
        String roleMessage = fields.get("destination-message");

        if ("base64-bytes".equals(messageFormat)) {
            ForkliftSerializer serializer = connector.getDefaultSerializer();

            roleMessage = serializer.deserializeForSource(
                destinationSource, Base64.getDecoder().decode(roleMessage));
        } else if(!"raw-string".equals(messageFormat)) {
            log.error("Unrecognized message format '{}' while retrying message", messageFormat);
        }

        try {
            producer.send(roleMessage);
        } catch (Exception e) {
            log.error("Unable to resend msg", e);
            // TODO schedule this message to run again.
            return;
        }

        complete.accept(id);
    }
}
