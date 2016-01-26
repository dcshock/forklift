package forklift.retry;

import forklift.concurrent.Callback;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.message.Header;
import forklift.producers.ForkliftProducerI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryRunnable implements Runnable {
    public static final Logger log = LoggerFactory.getLogger(RetryRunnable.class);

    private RetryMessage msg;
    private ForkliftConnectorI connector;
    private Callback<RetryMessage> complete;

    public RetryRunnable(RetryMessage msg, ForkliftConnectorI connector, Callback<RetryMessage> complete) {
        this.msg = msg;
        this.connector = connector;
        this.complete = complete;
    }

    @Override
    public void run() {
        ForkliftProducerI producer = null;
        try {
        if (msg.getQueue() != null)
            producer = connector.getQueueProducer(msg.getQueue());
        else if (msg.getTopic() != null)
            producer = connector.getTopicProducer(msg.getTopic());
        } catch (Throwable e) {
            log.error("", e);
            e.printStackTrace();
            return;
        }

        log.info("Retrying {}", msg);
        try {
            producer.send(toForkliftMessage(msg));
        } catch (Exception e) {
            log.warn("Unable to resend msg", e);
            // TODO schedule this message to run again.
            return;
        }

        complete.handle(msg);
    }

    private ForkliftMessage toForkliftMessage(RetryMessage msg) {
        final ForkliftMessage forkliftMsg = new ForkliftMessage();
        msg.getHeaders().put(Header.CorrelationId, msg.getMessageId());
        forkliftMsg.setHeaders(msg.getHeaders());
        forkliftMsg.setMsg(msg.getText());
        forkliftMsg.setProperties(msg.getProperties());
        return forkliftMsg;
    }
}
