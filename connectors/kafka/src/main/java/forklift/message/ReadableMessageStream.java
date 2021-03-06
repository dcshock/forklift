package forklift.message;

/**
 * A stream of {@link forklift.message.KafkaMessage} which can be read from.
 */
public interface ReadableMessageStream {
    /**
     * Blocking method to retrieve the next available record for a topic.  Messages are retrieved
     * in FIFO order within their topic.
     *
     * @param topic   the topic the retrieved message should belong to
     * @param timeout how long to wait for a message in milliseconds
     * @return a message if one is available, else null
     * @throws InterruptedException if interrupted
     */
    KafkaMessage nextRecord(String topic, long timeout) throws InterruptedException;
}
