package forklift.consumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.controller.KafkaController;
import forklift.message.ReadableMessageStream;

import java.util.concurrent.TimeUnit;

/**
 * Retrieves messages from a {@link forklift.controller.KafkaController}
 */
public class KafkaTopicConsumer implements ForkliftConsumerI {
    private final String topic;
    private final KafkaController controller;
    private final ReadableMessageStream messageStream;
    private volatile boolean topicAdded = false;

    public KafkaTopicConsumer(String topic, KafkaController controller) {
        this.topic = topic;
        this.controller = controller;
        this.messageStream = controller.getMessageStream();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Retrieves the {@link forklift.message.MessageStream#nextRecord(String, long) nextRecord} from the messageStream.
     * If no record is available within the specified timeout, null is returned.
     *
     * @param timeout the time in milliseconds to wait for a record to become available
     * @return a message if one is available, else null
     * @throws ConnectorException if the process is interrupted or the controller is no longer running
     */
    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        //ensure that the controller is still running
        if (!controller.isRunning()) {
            throw new ConnectorException("Connection to Kafka Controller lost");
        }
        try {
            return this.messageStream.nextRecord(this.topic, timeout);
        } catch (InterruptedException e) {
            throw new ConnectorException("Kafka message receive interrupted");
        }
    }


    /**
     * {@inheritDoc}
     * <p>
     * Closes this consumer and stops the controller.
     * </p>
     */
    @Override
    public void close() {
        try {
            controller.stop(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
