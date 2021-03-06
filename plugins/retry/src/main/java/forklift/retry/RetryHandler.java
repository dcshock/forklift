package forklift.retry;

import forklift.Forklift;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import forklift.file.FileScanner;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.function.Consumer;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Handles retries for consumers that have been annotated with Retry
 * <br>
 * Properties: forklift-retry-max-retries-exceeded, forklift-retry-max-retries, forklift-retry-count
 * @author mconroy
 *
 */
public class RetryHandler {
    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private File dir;
    private Forklift forklift;
    private ObjectMapper mapper;
    private ScheduledExecutorService executor;
    private Consumer<RetryMessage> cleanup;

    public RetryHandler(Forklift forklift) {
        this(forklift, new File("."));
    }

    public RetryHandler(Forklift forklift, File dir) {
        this.dir = dir;
        this.forklift = forklift;
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.executor = Executors.newScheduledThreadPool(1);

        // Cleanup after a retry is completed.
        cleanup = (msg) -> {
            log.info("Cleaning up persistent file {}", msg.getPersistedPath());
            final File f = new File(msg.getPersistedPath());
            if (f.exists())
                f.delete();
        };

        // Load up any existing messages.
        new FileScanner(dir).scan().stream()
            .filter(result -> result.getFilename().startsWith("retry") && result.getFilename().endsWith(".msg"))
            .forEach(result -> {
                try {
                    final RetryMessage retryMessage = mapper.readValue(new File(dir, result.getFilename()), RetryMessage.class);
                    /*executor.schedule(new RetryRunnable(retryMessage, forklift.getConnector(), cleanup),
                        Long.parseLong(Integer.toString(Integer.parseInt(retryMessage.getProperties().get("forklift-retry-timeout")))), TimeUnit.SECONDS);*/
                } catch (Exception e) {
                    log.error("Unable to read file {}", result.getFilename());
                }
            });
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Retry.class)
    public void error(MessageRunnable mr, Retry retry) {
        final ForkliftMessage msg = mr.getMsg();

        // Read props of the message to see what we need to do with retry counts
        final Map<String, String> props = msg.getProperties();

        // Handle retries
        Integer retryCount;
        try {
            retryCount = Integer.parseInt(props.get("forklift-retry-count"));
        } catch (NumberFormatException e) {
            retryCount = 0;
        }

        retryCount++;
        if (retryCount > retry.maxRetries()) {
            props.put("forklift-retry-max-retries-exceeded", "true");
            return;
        } else {
            props.put("forklift-retry-max-retries", "" + retry.maxRetries());
            props.put("forklift-retry-count", "" + retryCount);
            props.put("forklift-retry-timeout", "" + retry.timeout());
        }

        try {
            final RetryMessage retryMessage = new RetryMessage();
            retryMessage.setMessageId(msg.getId());
            retryMessage.setText(msg.getMsg());
            retryMessage.setHeaders(msg.getHeaders());
            retryMessage.setStep(ProcessStep.Error);
            retryMessage.setProperties(msg.getProperties());

            mr.getConsumer().getSource()
                .accept(QueueSource.class, queue -> retryMessage.setQueue(queue.getName()))
                .accept(TopicSource.class, topic -> retryMessage.setTopic(topic.getName()));

            // Only persist retries if it was requested.
            if (retry.persistent()) {
                BufferedWriter writer = null;
                try {
                    // Create a new persisted file, and store the path so we can clean up later after the message is pushed on the queue.
                    final File file = new File(dir, "retry." + mr.getMsg().getId() + ".msg");
                    retryMessage.setPersistedPath(file.getAbsolutePath());

                    // Write the message to a file so we don't lose it if a restart occurs.
                    writer = Files.newWriter(file, Charset.forName("UTF-8"));
                    writer.write(mapper.writeValueAsString(retryMessage));
                } finally {
                    if (writer != null)
                        writer.close();
                }
            }

            // Scheule the message to be retried.
            // executor.schedule(new RetryRunnable(retryMessage, forklift.getConnector(), cleanup), retry.timeout(), TimeUnit.SECONDS);
        } catch (IOException ignored) {
        }
    }
}
