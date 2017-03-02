package forklift.retry;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.gson.JsonObject;
import forklift.concurrent.Callback;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.message.Header;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

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

    private final ForkliftConnectorI connector;
    private final ScheduledExecutorService executor;
    private final JestClient client;
    private final ObjectMapper mapper;
    private final Callback<RetryMessage> cleanup;

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

    public RetryES(ForkliftConnectorI connector, boolean ssl, String hostname, int port, boolean runRetries) {
        this.connector = connector;
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
        cleanup = new Callback<RetryMessage>() {
            @Override
            public void handle(RetryMessage msg) {
                try {
                    client.execute(new Delete.Builder(msg.getMessageId()).index("forklift-retry").type("msg").build());
                } catch (IOException e) {
                    log.error("Unable to cleanup retry: {}", msg.getMessageId(), e);
                }
            }
        };

        /*
         * Query for all available retries and schedule them on this node.
         * Since this functionality can cause duplicate messages we allow the utilizing process
         * to shut this functionality off.
         */
        if (runRetries) {
            try {
                final String query = "{\"size\" : \"10000\", \"query\" : { \"match_all\" : {} }}";
                final Search search = new Search.Builder(query).addIndex("forklift-retry").build();
                final SearchResult results = client.execute(search);
                if (results != null) {
                    try {
                        for (Hit<JsonObject, Void> msg : results.getHits(JsonObject.class)) {
                            try {
                                final RetryMessage
                                                retryMessage =
                                                mapper.readValue(msg.source.get("forklift-retry-msg").getAsString(), RetryMessage.class);
                                log.info("Retrying: {}", retryMessage);
                                executor.schedule(new RetryRunnable(retryMessage, connector, cleanup),
                                                  Long.parseLong(Integer.toString((int)retryMessage.getProperties().get("forklift-retry-timeout"))), TimeUnit.SECONDS);
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
        }
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Retry.class)
    public void msg(MessageRunnable mr, Retry retry) {
        final ForkliftMessage msg = mr.getMsg();

        final Map<String, String> fields = new HashMap<String, String>();
        fields.put("text", msg.getMsg());
        fields.put("step", ProcessStep.Error.toString());

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
            final Map<String, Object> props = msg.getProperties();

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
                props.put("forklift-retry-max-retries-exceeded", Boolean.TRUE);
                return;
            } else {
                props.put("forklift-retry-max-retries", retry.maxRetries());
                props.put("forklift-retry-count", retryCount);
                props.put("forklift-retry-timeout", retry.timeout());
            }

            // Map in properties
            for (String key : props.keySet()) {
                final Object val = msg.getProperties().get(key);
                if (val != null)
                    fields.put(key.toString(), msg.getProperties().get(key).toString());
            }
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

        try {
            final RetryMessage retryMsg = buildRetry(mr, retry, id);
            fields.put("forklift-retry-msg", mapper.writeValueAsString(retryMsg));

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
            executor.schedule(new RetryRunnable(retryMsg, connector, cleanup), retry.timeout(), TimeUnit.SECONDS);
        } catch (IOException e) {
            log.error("Unable to index retry: {}", fields.toString(), e);
        }
    }

    public RetryMessage buildRetry(MessageRunnable mr, Retry retry, String id) {
        final ForkliftMessage msg = mr.getMsg();

        final RetryMessage retryMessage = new RetryMessage();
        retryMessage.setMessageId(id);
        retryMessage.setText(msg.getMsg());
        retryMessage.setHeaders(msg.getHeaders());
        retryMessage.setStep(ProcessStep.Error);
        retryMessage.setProperties(msg.getProperties());

        if (mr.getConsumer().getQueue() != null)
            retryMessage.setQueue(mr.getConsumer().getQueue().value());

        if (mr.getConsumer().getTopic() != null)
            retryMessage.setTopic(mr.getConsumer().getTopic().value());

        return retryMessage;
    }
}
