package forklift.replay;

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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builds the fields for replay message logs that are put into Elasticsearch.
 */
public class ReplayLogBuilder {
    final Map<String, String> fields = new HashMap<>();
    public ReplayLogBuilder(ForkliftMessage msg, Consumer consumer, List<String> errorList, ForkliftConnectorI connector, Replay replay, ProcessStep step) {
        final Map<String, String> props = msg.getProperties();
        Optional<ForkliftSerializer> serializer = Optional.ofNullable(connector.getDefaultSerializer());

        final String connectorName = connector.getClass().getSimpleName();
        final String sourceDescription = consumer.getSource().toString();
        final Optional<String> errors = errorList.stream().reduce((a, b) -> a + ":" + b);

        final Optional<RoleInputSource> declaredRoleSource = consumer.getRoleSources(RoleInputSource.class).findFirst();
        final String role = declaredRoleSource.map(source -> source.getRole())
            .orElseGet(() -> fallbackRole(replay, consumer.getMsgHandler()));
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


        if (!props.containsKey("forklift-replay-step-count")) { // new messages are version 3
            props.putIfAbsent("forklift-replay-version", "3");
        }
        final long stepCount = Integer.parseInt(props.getOrDefault("forklift-replay-step-count", "0")) + 1;
        props.put("forklift-replay-step-count", "" + stepCount);
        props.putIfAbsent("first-processed-date", LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE));

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

    private String fallbackRole(Replay replay, Class<?> msgHandler) {
        return Optional.of(replay.role())
            .filter(role -> !role.isEmpty())
            .orElse(msgHandler.getSimpleName());
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public long getStepCount() {
        return Long.parseLong(fields.get("forklift-replay-step-count"));
    }
}
