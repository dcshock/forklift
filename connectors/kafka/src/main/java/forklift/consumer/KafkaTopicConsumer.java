package forklift.consumer;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.controller.KafkaController;
import forklift.message.ReadableMessageStream;

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
     * <p>
     * <strong>Note:</strong> Because we do not wish to poll for kafka topics until we are actively receiving messages, this method is
     * also responsible for calling {@link forklift.controller.KafkaController#addTopic(String) addTopic} on the kafkaController.
     *
     * @param timeout the time in milliseconds to wait for a record to become available
     * @return a message if one is available, else null
     * @throws ConnectorException if the process is interrupted or the controller is no longer running
     */
    @Override
    public ForkliftMessage receive(long timeout) throws ConnectorException {
        addTopicIfMissing();
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

    private void addTopicIfMissing() {
        if (!topicAdded) {
            synchronized (this) {
                if (!topicAdded) {
                    controller.addTopic(topic);
                    topicAdded = true;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Removes this consumer's topic from the controller.  Future calls to {@link #receive(long) receive} will re-add the
     * topic to the controller.
     * </p>
     */
    @Override
    public void close() {
        controller.removeTopic(topic);
    }
}
