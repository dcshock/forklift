package forklift.retry;

import forklift.Forklift;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
import java.util.HashSet;
import java.util.Map;
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

    /**
     * Creates a new instance of the retry plugin that temporarily writes logs to elasticsearch using
     * the REST interface.
     *
     * @param forklift the forklift server using this plugin
     * @param ssl whether or not to connect to elasticsearch using http or https
     * @param hostname the hostname to use to connect to elasticsearch
     * @param port the REST port for elasticsearch
     * @param runRetries whether or not re-process messages found in the retry log in elasticsearch;
     *      may cause duplicate messages if triggered when another forklift server is already processing
     *      a message
     */
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
                                final String msgConnector = fields.get("destination-connector");

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

    @LifeCycle(value=ProcessStep.Error, annotation=Retry.class)
    public void msg(MessageRunnable mr, Retry retry) {
        final ForkliftMessage msg = mr.getMsg();
        final String id = msg.getId();
        final ForkliftConnectorI connector = mr.getConsumer().getForklift().getConnector();
        final String connectorName = connector.getClass().getSimpleName();

        // Get the message id. If there is no id we ignore the retry...
        if (id == null || id.isEmpty()) {
            log.error("Could not retry message; no message id for connector: {}, message: {}", connectorName, msg.getMsg());
            return;
        }

        final RetryLogBuilder logBuilder = new RetryLogBuilder(msg, mr.getConsumer(), mr.getErrors(), connector, retry);
        final Map<String, String> fields = logBuilder.getFields();

        if (logBuilder.logProduced()) {
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
}
