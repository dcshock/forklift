package forklift.retry;

import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.ForkliftSerializer;
import forklift.consumer.Consumer;
import forklift.consumer.ProcessStep;
import forklift.consumer.wrapper.RoleInputMessage;
import forklift.message.Header;
import forklift.source.ActionSource;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.TopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builds the fields to insert into Elasticsearch for retry logs.
 */
public class RetryLogBuilder {
    private Map<String, String> fields = new HashMap<>();
    public RetryLogBuilder(ForkliftMessage msg, Consumer consumer, List<String> errorList, ForkliftConnectorI connector, Retry retry) {
        final String connectorName = connector.getClass().getSimpleName();

        // Map in headers
        for (Header key : msg.getHeaders().keySet()) {
            // Skip the correlation id because it is already set in the user id field.
            if (key == Header.CorrelationId)
                continue;

            final Object val = msg.getHeaders().get(key);
            if (val != null)
                fields.put(key.toString(), msg.getHeaders().get(key).toString());
        }

        /*
         * Properties handling
         */
        {
            // Read props of the message to see what we need to do with retry counts
            final Map<String, String> props = msg.getProperties();
            final String sourceDescription = consumer.getSource().toString();

            props.putIfAbsent("source-description", sourceDescription);

            // Determine the current retry count. We have to handle string or integer input types
            // since stomp doesn't differentiate the two.
            Integer retryCount;
            final Object obj = props.get("forklift-retry-count");
            if (obj instanceof String)
                retryCount = Integer.parseInt((String)obj);
            else if (obj instanceof Integer)
                retryCount = (Integer)obj;
            else
                retryCount = null;

            // Handle retries
            if (retryCount == null)
                retryCount = 1;
            else
                retryCount++;
            if (retryCount > retry.maxRetries()) {
                props.put("forklift-retry-max-retries-exceeded", "true");
                fields = null;
                return;
            } else {
                props.put("forklift-retry-max-retries", "" + retry.maxRetries());
                props.put("forklift-retry-count", "" + retryCount);
                props.put("forklift-retry-timeout", "" + retry.timeout());
            }

            // Map in properties
            for (String key : props.keySet()) {
                final Object val = msg.getProperties().get(key);
                if (val != null)
                    fields.put(key.toString(), msg.getProperties().get(key).toString());
            }
        }

        Optional<ForkliftSerializer> serializer = Optional.ofNullable(connector.getDefaultSerializer());

        final Optional<String> errors = errorList.stream().reduce((a, b) -> a + ":" + b);

        final Optional<RoleInputSource> declaredRoleSource = consumer.getRoleSources(RoleInputSource.class).findFirst();
        final String role = declaredRoleSource.map(source -> source.getRole())
            .orElseGet(() -> fallbackRole(retry, consumer.getMsgHandler()));
        final RoleInputSource roleSource = declaredRoleSource.orElseGet(() -> new RoleInputSource(role));
        final ActionSource actionSource = roleSource.getActionSource(connector);

        final RoleInputMessage roleMessage = RoleInputMessage.fromForkliftMessage(role, msg);
        final String roleMessageJson = roleMessage.toString();

        // process-level details
        fields.put("role", role);
        fields.put("step", ProcessStep.Error.toString());
        errors.ifPresent(errorString -> fields.put("errors", errorString));
        fields.put("time", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        fields.put("forklift-retry-version", "2");

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
    }

    private String fallbackRole(Retry retry, Class<?> msgHandler) {
        return Optional.of(retry.role())
            .filter(role -> !role.isEmpty())
            .orElse(msgHandler.getSimpleName());
    }

    public Map<String, String> getFields() {
        return fields;
    }
}
