package forklift.retry;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.connectors.ForkliftSerializer;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.consumer.wrapper.RoleInputMessage;
import forklift.decorators.LifeCycle;
import forklift.message.Header;
import forklift.source.ActionSource;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.TopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Handles retries for consumers that have been annotated with Retry
 * Utilizes elastic search to store retry data so that forklift can be ran on multiple nodes.
 * <br>
 * Properties: forklift-retry-max-retries-exceeded, forklift-retry-max-retries, forklift-retry-count
 * @author mconroy
 *
 */
public class RetryES {
    private static final Logger log = LoggerFactory.getLogger(RetryES.class);

    private final Forklift forklift;
    private final ScheduledExecutorService executor;
    private final JestClient client;
    private final ObjectMapper mapper;
    private final Consumer<String> cleanup;

    /*
     * Just a test case.
     */
//    public static void main(String[] args) throws IOException {
//        final JestClientFactory factory = new JestClientFactory();
//        factory.setHttpClientConfig(
//            new HttpClientConfig.Builder("http://localhost:9200")
//               .multiThreaded(true)
//               .build());
//        JestClient client = factory.getObject();
//
//        final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule())
//                                                      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
//
//        final String query = "{\"size\" : \"10000\", \"query\" : { \"match_all\" : {} }}";
//        final Search search = new Search.Builder(query).addIndex("forklift-retry").addType("msg").build();
//        final SearchResult results = client.execute(search);
//
//        log.error(results.getErrorMessage());
//        log.info("{}", results.getHits(JsonObject.class));
//
//        for (Hit<JsonObject, Void> msg : results.getHits(JsonObject.class)) {
//            try {
//                final RetryMessage retryMessage = mapper.readValue(msg.source.get("forklift-retry-msg").getAsString(), RetryMessage.class);
//                log.info("Retrying: {}", retryMessage);
//            } catch (Exception e) {
//                log.error("Unable to read result {}", msg.source);
//            }
//        }
//    }

    public RetryES(Forklift forklift, boolean ssl, String hostname, int port, boolean runRetries) {
        this.forklift = forklift;
        this.executor = Executors.newScheduledThreadPool(1);
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        final String prefix;
        if (ssl)
            prefix = "https://";
        else
            prefix = "http://";

        final JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
            new HttpClientConfig.Builder(prefix + hostname + ":" + port)
               .multiThreaded(true)
               .build());
        client = factory.getObject();

        // Cleanup after a retry is completed.
        cleanup = (id) -> {
            if (id == null) {
                log.error("Cannot cleanup retry with null id: {}", id);
                return;
            }

            try {
                client.execute(new Delete.Builder(id).index("forklift-retry").type("msg").build());
            } catch (IOException e) {
                log.error("Unable to cleanup retry: {}", id, e);
            }
        };

        /*
         * Query for all available retries and schedule them on this node.
         * Since this functionality can cause duplicate messages we allow the utilizing process
         * to shut this functionality off.
         */
        if (runRetries) {
            final String givenConnector = forklift.getConnector().getClass().getSimpleName();
            boolean retriesSkipped = false;
            Set<String> skippedConnectors = new HashSet<>();

            try {
                final String query = "{\"size\" : \"10000\", \"query\" : { \"match_all\" : {} }}";
                final Search search = new Search.Builder(query).addIndex("forklift-retry").build();
                final SearchResult results = client.execute(search);
                if (results != null) {
                    try {
                        for (Hit<Map, Void> msg : results.getHits(Map.class)) {
                            try {
                                final Map<String, String> fields = msg.source;
                                final String msgConnector = fields.get("connectorName");

                                if (msgConnector != null &&
                                    !givenConnector.equals(msgConnector)) {
                                    skippedConnectors.add(msgConnector);
                                    retriesSkipped = true;
                                } else {
                                    log.info("Retrying: {}", fields);
                                    new RetryRunnable(msg.id, fields, forklift.getConnector(), cleanup).run();
                                }
                            } catch (Exception e) {
                                log.error("Unable to read result {}", msg.source);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Search Results: {}", results.getJsonObject().toString());
                        log.error("", e);
                    }
                }
            } catch (Exception e) {
                log.error("@trevor - don't ignore this!", e);
            }

            if (retriesSkipped) {
                log.warn("Some retries weren't ran due to being targetted at a different connector: {}", skippedConnectors);
            }
        }
    }

    private String fallbackRole(Retry retry, Class<?> msgHandler) {
        return Optional.of(retry.role())
            .filter(role -> !role.isEmpty())
            .orElse(msgHandler.getSimpleName());
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Retry.class)
    public void msg(MessageRunnable mr, Retry retry) {
        final ForkliftMessage msg = mr.getMsg();
        final String id = msg.getId();
        final ForkliftConnectorI connector = mr.getConsumer().getForklift().getConnector();
        final String connectorName = connector.getClass().getSimpleName();
        final Map<String, String> fields = new HashMap<>();

        // Get the message id. If there is no id we ignore the retry...
        if (id == null) {
            log.error("Could not retry message; no message id for connector: {}, message: {}", connectorName, msg.getMsg());
            return;
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

        /*
         * Properties handling
         */
        {
            // Read props of the message to see what we need to do with retry counts
            final Map<String, String> props = msg.getProperties();
            final String sourceDescription = mr.getConsumer().getSource().toString();

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

        Optional<ForkliftSerializer> serializer = Optional.empty();
        if (connector instanceof ForkliftSerializer) {
            serializer = Optional.of((ForkliftSerializer) connector);
        }

        final Optional<String> errors = mr.getErrors().stream().reduce((a, b) -> a + ":" + b);

        final Optional<RoleInputSource> declaredRoleSource = mr.getConsumer().getRoleSources(RoleInputSource.class).findFirst();
        final String role = declaredRoleSource.map(source -> source.getRole())
            .orElseGet(() -> fallbackRole(retry, mr.getConsumer().getMsgHandler()));
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

        for (int i = 0; i < 3; i++) {
            try {
                // Index the new information.
                client.execute(new Index.Builder(fields).index("forklift-retry").type("msg").id(id).build());
                break;
            } catch (IOException e) {
                if (i == 2)
                    log.error("Unable to index retry: {}", fields.toString(), e);
            }
        }

        // Scheule the message to be retried.
        executor.schedule(new RetryRunnable(id, fields, connector, cleanup), retry.timeout(), TimeUnit.SECONDS);
    }
}
