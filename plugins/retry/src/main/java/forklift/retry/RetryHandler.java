package forklift.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Files;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

/**
 * Handles retries for consumers that have been annotated with Retry
 * <br>
 * Properties: forklift-retry-max-retries-exceeded, forklift-retry-max-retries, forklift-retry-count
 * @author mconroy
 *
 */
public class RetryHandler {
    private static final Logger log = LoggerFactory.getLogger(RetryHandler.class);

    private ObjectMapper mapper;
    private ScheduledExecutorService executor;

    public RetryHandler() {
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.executor = Executors.newScheduledThreadPool(1);
    }

    @LifeCycle(value=ProcessStep.Error, annotation=Retry.class)
    public void error(MessageRunnable mr, Retry retry) {
        final ForkliftMessage msg = mr.getMsg();

        // Read props of the message to see what we need to do with retry counts
        final Map<String, Object> props = msg.getProperties();

        // Handle retries
        Integer retryCount = (Integer)props.get("forklift-retry-count");
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
        }

        try {
            final RetryMessage retryMessage = new RetryMessage();
            retryMessage.setMessageId(msg.getJmsMsg().getJMSMessageID());
            retryMessage.setText(msg.getMsg());
            retryMessage.setHeaders(msg.getHeaders());
            retryMessage.setStep(ProcessStep.Error);
            retryMessage.setProperties(msg.getProperties());

            if (mr.getConsumer().getQueue() != null)
                retryMessage.setQueue(mr.getConsumer().getQueue().value());

            if (mr.getConsumer().getTopic() != null)
                retryMessage.setTopic(mr.getConsumer().getTopic().value());

            // Only persist retries if it was requested.
            if (retry.persistent()) {
                BufferedWriter writer = null;
                try {
                    // Create a new persisted file, and store the path so we can clean up later after the message is pushed on the queue.
                    final File file = new File("retry." + mr.getMsg().getJmsMsg().getJMSMessageID().toString() + ".msg");
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
            executor.schedule(
                () -> {
                    log.info("Retrying {}", retryMessage);
                    log.info("Cleaning up persistent file {}", retryMessage.getPersistedPath());
                    final File f = new File(retryMessage.getPersistedPath());
                    if (f.exists())
                        f.delete();
                }, retry.timeout(), TimeUnit.SECONDS);
        } catch (JMSException | IOException ignored) {
        }
    }
}